package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// These tests cover a quota exhaustion after the HTTP response is committed
// (200, text/event-stream): the usage limit has to travel in the terminal
// error event because the status and headers are already sent.

const quotaStreamProvider = "quota-usage-limit-stream-fake"

// streamSurface is a client surface plus how its stream frames look.
type streamSurface struct {
	quotaSurface
	codexClient bool
}

var streamSurfaces = []streamSurface{
	{quotaSurface: quotaSurfaces[0]},
	{quotaSurface: quotaSurface{name: "responses-codex", path: "/v1/responses", body: quotaSurfaces[0].body}, codexClient: true},
	{quotaSurface: quotaSurfaces[1]},
	{quotaSurface: quotaSurfaces[2]},
}

// partialFrame is output the client receives before the failure.
func partialFrame(format string) []byte {
	switch format {
	case "openai":
		return []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`)
	case "claude":
		return []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n")
	default:
		return []byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial\"}\n\n")
	}
}

// upstreamQuotaFrame is the upstream's own quota failure event, forwarded by an
// executor right before its error chunk (as Codex duplex streams do).
func upstreamQuotaFrame(format string) []byte {
	switch format {
	case "openai":
		return []byte(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":7200}}`)
	case "claude":
		return []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"The usage limit has been reached\"}}\n\n")
	default:
		return []byte("event: error\ndata: {\"type\":\"error\",\"status\":429,\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\",\"resets_in_seconds\":7200}}\n\n")
	}
}

// streamScript says how a credential's stream fails.
type streamScript struct {
	// upstreamFrame forwards the upstream quota failure event before err.
	upstreamFrame bool
	// gated makes ExecuteStream wait until a keep-alive was sent and then
	// fail before any output.
	gated bool
	err   error
}

type quotaStreamExecutor struct {
	mu      sync.Mutex
	scripts map[string]streamScript
	gate    chan struct{}
}

func (*quotaStreamExecutor) Identifier() string { return quotaStreamProvider }

func (e *quotaStreamExecutor) script(auth *coreauth.Auth) (streamScript, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	script, ok := e.scripts[auth.ID]
	return script, ok
}

func (e *quotaStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *quotaStreamExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	script, ok := e.script(auth)
	if !ok {
		return nil, errors.New("unexpected credential " + auth.ID)
	}
	if script.gated {
		select {
		case <-e.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, script.err
	}
	format := opts.SourceFormat.String()
	chunks := make(chan coreexecutor.StreamChunk, 3)
	chunks <- coreexecutor.StreamChunk{Payload: partialFrame(format)}
	if script.upstreamFrame {
		chunks <- coreexecutor.StreamChunk{Payload: upstreamQuotaFrame(format)}
	}
	chunks <- coreexecutor.StreamChunk{Err: script.err}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*quotaStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*quotaStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*quotaStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type quotaStreamHarness struct {
	router  *gin.Engine
	manager *coreauth.Manager
	model   string
	authIDs []string
	// tick, when set, delivers one pre-first-token keep-alive tick; gate
	// then lets a gated credential fail.
	tick chan time.Time
	gate chan struct{}
}

// newQuotaStreamHarness registers one credential per script. With keepAlive,
// pre-first-token keep-alives are on and driven by h.tick.
func newQuotaStreamHarness(t *testing.T, mode string, keepAlive bool, scripts ...streamScript) *quotaStreamHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	executor := &quotaStreamExecutor{scripts: make(map[string]streamScript), gate: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	model := "quota-stream-" + strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	h := &quotaStreamHarness{manager: manager, model: model, gate: executor.gate}
	for i, script := range scripts {
		authID := fmt.Sprintf("%s-auth-%d", model, i)
		executor.scripts[authID] = script
		if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: authID, Provider: quotaStreamProvider, Status: coreauth.StatusActive}); err != nil {
			t.Fatalf("register %s: %v", authID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(authID, quotaStreamProvider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
		h.authIDs = append(h.authIDs, authID)
	}

	cfg := &sdkconfig.SDKConfig{}
	if keepAlive {
		cfg.Streaming.KeepAliveSeconds = 1
		cfg.Streaming.SendKeepAliveBeforeFT = true
		h.tick = make(chan time.Time)
		var calls int
		var callsMu sync.Mutex
		restore := handlers.SetStreamingKeepAliveTickerFactoryForTest(func(time.Duration) (<-chan time.Time, func()) {
			callsMu.Lock()
			defer callsMu.Unlock()
			calls++
			if calls == 1 {
				return h.tick, func() {}
			}
			// Later tickers (after the first token) never fire.
			return make(chan time.Time), func() {}
		})
		t.Cleanup(restore)
	}
	base := handlers.NewBaseAPIHandlers(cfg, manager)
	router := gin.New()
	router.POST("/v1/responses", openai.NewOpenAIResponsesAPIHandler(base).Responses)
	router.POST("/v1/chat/completions", openai.NewOpenAIAPIHandler(base).ChatCompletions)
	router.POST("/v1/messages", claude.NewClaudeCodeAPIHandler(base).ClaudeMessages)
	h.router = router
	return h
}

// coolWithoutEvidence cools a credential down with an unclassified 429, so the
// pool never qualifies for the usage limit.
func (h *quotaStreamHarness) coolWithoutEvidence(t *testing.T, authID string) {
	t.Helper()
	retry := 2 * time.Hour
	h.manager.MarkResult(context.Background(), coreauth.Result{
		AuthID:     authID,
		Provider:   quotaStreamProvider,
		Model:      h.model,
		Error:      &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: "slow down"},
		RetryAfter: &retry,
	})
}

func (h *quotaStreamHarness) do(surface streamSurface) *httptest.ResponseRecorder {
	body := ojson.NewDocString(surface.body)
	for path, value := range map[string]any{"model": h.model, "stream": true} {
		if errSet := body.Set(path, value); errSet != nil {
			panic(errSet)
		}
	}
	request := httptest.NewRequest(http.MethodPost, surface.path, strings.NewReader(body.String()))
	request.Header.Set("Content-Type", "application/json")
	if surface.codexClient {
		request.Header.Set("Originator", "codex_cli_rs")
	}
	recorder := httptest.NewRecorder()
	if h.tick == nil {
		h.router.ServeHTTP(recorder, request)
		return recorder
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.router.ServeHTTP(recorder, request)
	}()
	// The handler writes the keep-alive before it reads the next event, so
	// the failure released after this send arrives on a committed response.
	h.tick <- time.Now()
	close(h.gate)
	<-done
	return recorder
}

// errorEvents returns the JSON of every terminal error event in an SSE body.
func errorEvents(surface streamSurface, body string) []string {
	var events []string
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		switch surface.name {
		case "chat":
			if data, ok := strings.CutPrefix(line, "data: "); ok && ojson.Get(data, "error").IsObject() {
				events = append(events, data)
			}
		default:
			if line != "event: error" && line != "event: response.failed" {
				continue
			}
			if i+1 < len(lines) {
				events = append(events, strings.TrimPrefix(lines[i+1], "data: "))
			}
		}
	}
	return events
}

func assertStreamUsageLimit(t *testing.T, surface streamSurface, recorder *httptest.ResponseRecorder, wantPartial bool) {
	t.Helper()
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200; body=%s", recorder.Code, body)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream; body=%s", got, body)
	}
	if got := strings.Count(body, "partial"); wantPartial && got != 1 {
		t.Fatalf("partial output appears %d times, want once; body=%s", got, body)
	} else if !wantPartial && got != 0 {
		t.Fatalf("partial output appears %d times, want none; body=%s", got, body)
	}
	events := errorEvents(surface, body)
	if len(events) != 1 {
		t.Fatalf("error events = %d, want exactly one; body=%s", len(events), body)
	}
	event := events[0]
	wantReset := time.Now().Add(2 * time.Hour).Unix()
	errorNode := ojson.Get(event, "error")
	switch {
	case surface.codexClient:
		if got := ojson.Get(event, "type").String(); got != "response.failed" {
			t.Fatalf("type = %q, want response.failed; event=%s", got, event)
		}
		errorNode = ojson.Get(event, "response.error")
	case surface.claude:
		if got := ojson.Get(event, "type").String(); got != "error" {
			t.Fatalf("type = %q, want error; event=%s", got, event)
		}
		if got := errorNode.Get("type").String(); got != "rate_limit_error" {
			t.Fatalf("error.type = %q, want rate_limit_error; event=%s", got, event)
		}
		if message := errorNode.Get("message").String(); !claudeResetSuffix.MatchString(message) {
			t.Fatalf("error.message = %q, want a (reset after <duration>) suffix", message)
		}
	case surface.name == "responses":
		if got := ojson.Get(event, "type").String(); got != "error" {
			t.Fatalf("type = %q, want error; event=%s", got, event)
		}
	}
	if !surface.claude {
		if got := errorNode.Get("type").String(); got != "usage_limit_reached" {
			t.Fatalf("error.type = %q, want usage_limit_reached; event=%s", got, event)
		}
		if got := errorNode.Get("limit_window_minutes").Int(); got != 300 {
			t.Fatalf("error.limit_window_minutes = %d, want 300; event=%s", got, event)
		}
	}
	if got := errorNode.Get("resets_at").Int(); got < wantReset-5 || got > wantReset+5 {
		t.Fatalf("error.resets_at = %d, want about %d; event=%s", got, wantReset, event)
	}
	if retryable := errorNode.Get("retryable"); !retryable.Exists() || retryable.Bool() {
		t.Fatalf("error.retryable = %s, want false; event=%s", retryable.Raw, event)
	}
	if surface.name == "chat" && !strings.HasSuffix(body, event+"\n\ndata: [DONE]\n\n") {
		t.Fatalf("chat stream does not end with the error then [DONE]; body=%s", body)
	}
}

// TestUsageLimitAfterCommittedStream covers a quota failure after output was
// streamed and after only keep-alives were sent, on every surface.
func TestUsageLimitAfterCommittedStream(t *testing.T) {
	for _, surface := range streamSurfaces {
		t.Run(surface.name+"/mid-stream", func(t *testing.T) {
			h := newQuotaStreamHarness(t, "usage-limit", false, streamScript{err: codexUsageLimit(2 * time.Hour)})
			assertStreamUsageLimit(t, surface, h.do(surface), true)
		})
		t.Run(surface.name+"/upstream-frame", func(t *testing.T) {
			h := newQuotaStreamHarness(t, "usage-limit", false, streamScript{upstreamFrame: true, err: codexUsageLimit(2 * time.Hour)})
			assertStreamUsageLimit(t, surface, h.do(surface), true)
		})
		t.Run(surface.name+"/keep-alive-only", func(t *testing.T) {
			h := newQuotaStreamHarness(t, "usage-limit", true, streamScript{gated: true, err: codexUsageLimit(2 * time.Hour)})
			recorder := h.do(surface)
			if !strings.Contains(recorder.Body.String(), ": keep-alive\n\n") {
				t.Fatalf("no keep-alive was sent before the failure; body=%s", recorder.Body.String())
			}
			assertStreamUsageLimit(t, surface, recorder, false)
		})
	}
}

// TestCommittedStreamStaysUnchangedWithoutUsageLimit checks that usage-limit
// mode sends the same bytes as retryable mode when the pool does not qualify:
// another credential is cooling without quota evidence, the 429 is
// unclassified, or the reset is 5 minutes away or less.
func TestCommittedStreamStaysUnchangedWithoutUsageLimit(t *testing.T) {
	cases := []struct {
		name          string
		scripts       []streamScript
		coolSecond    bool
		upstreamFrame bool
	}{
		{name: "mixed-pool", scripts: []streamScript{{err: codexUsageLimit(2 * time.Hour)}, {err: errors.New("unused")}}, coolSecond: true},
		{name: "mixed-pool-upstream-frame", scripts: []streamScript{{upstreamFrame: true, err: codexUsageLimit(2 * time.Hour)}, {err: errors.New("unused")}}, coolSecond: true},
		{name: "unclassified-429", scripts: []streamScript{{err: genericRateLimit(2 * time.Hour)}}},
		{name: "short-reset", scripts: []streamScript{{err: codexUsageLimit(4 * time.Minute)}}},
	}
	for _, surface := range streamSurfaces {
		for _, tc := range cases {
			t.Run(surface.name+"/"+tc.name, func(t *testing.T) {
				run := func(mode string) *httptest.ResponseRecorder {
					h := newQuotaStreamHarness(t, mode, false, tc.scripts...)
					if tc.coolSecond {
						h.coolWithoutEvidence(t, h.authIDs[1])
					}
					return h.do(surface)
				}
				retryable := run("retryable")
				usageLimit := run("usage-limit")
				if retryable.Code != usageLimit.Code {
					t.Fatalf("status: retryable %d, usage-limit %d", retryable.Code, usageLimit.Code)
				}
				// The model name is per test, so both runs share it.
				if got, want := usageLimit.Body.String(), retryable.Body.String(); got != want {
					t.Fatalf("usage-limit body differs from retryable:\n got: %q\nwant: %q", got, want)
				}
				if strings.Contains(usageLimit.Body.String(), "resets_at") {
					t.Fatalf("non-qualifying pool reports a usage limit: %s", usageLimit.Body.String())
				}
			})
		}
	}
}

// TestRetryableModeCommittedStreamBytes pins the retryable-mode bytes of a
// committed stream that ends in quota exhaustion. The expected bodies are the
// ones the base branch sends.
func TestRetryableModeCommittedStreamBytes(t *testing.T) {
	for _, surface := range streamSurfaces {
		for _, upstreamFrame := range []bool{false, true} {
			name := surface.name + "/mid-stream"
			if upstreamFrame {
				name = surface.name + "/upstream-frame"
			}
			t.Run(name, func(t *testing.T) {
				h := newQuotaStreamHarness(t, "retryable", false, streamScript{upstreamFrame: upstreamFrame, err: codexUsageLimit(2 * time.Hour)})
				recorder := h.do(surface)
				want, ok := retryableCommittedStreamBodies[name]
				if !ok {
					t.Fatalf("no golden for %s; body=%q", name, recorder.Body.String())
				}
				if recorder.Code != http.StatusOK {
					t.Fatalf("status = %d, want 200", recorder.Code)
				}
				if got := recorder.Body.String(); got != want {
					t.Fatalf("body differs from base:\n got: %q\nwant: %q", got, want)
				}
			})
		}
	}
}

var retryableCommittedStreamBodies = map[string]string{
	"responses/mid-stream":           "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial\"}\n\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"The usage limit has been reached\",\"type\":\"usage_limit_reached\"},\"sequence_number\":2}\n\n",
	"responses/upstream-frame":       "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"The usage limit has been reached\",\"resets_in_seconds\":7200,\"type\":\"usage_limit_reached\"},\"sequence_number\":2}\n\n",
	"responses-codex/mid-stream":     "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial\"}\n\n\nevent: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"The usage limit has been reached\",\"type\":\"usage_limit_reached\"}}}\n\n",
	"responses-codex/upstream-frame": "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial\"}\n\nevent: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"The usage limit has been reached\",\"resets_in_seconds\":7200,\"type\":\"usage_limit_reached\"}}}\n\n",
	"chat/mid-stream":                "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\ndata: {\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\"}}\n\n",
	"chat/upstream-frame":            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\ndata: {\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\",\"resets_in_seconds\":7200}}\n\n",
	"messages/mid-stream":            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\"}}\n\n",
	"messages/upstream-frame":        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"The usage limit has been reached\"}}\n\n",
}
