package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const streamQuotaProvider = "usage-limit-stream-test"

// codexDuplexQuotaFrame is the upstream failure a Codex duplex stream forwards
// right before its error chunk.
const codexDuplexQuotaFrame = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":7200}}`

// streamQuotaExecutor streams response.created, then the scripted frames, then
// err, from a channel the test can pace with release.
type streamQuotaExecutor struct {
	frames  []string
	err     error
	release chan struct{}
}

func (*streamQuotaExecutor) Identifier() string { return streamQuotaProvider }

func (*streamQuotaExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (e *streamQuotaExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			select {
			case out <- chunk:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !send(cliproxyexecutor.StreamChunk{Payload: []byte(`{"type":"response.created","response":{"id":"resp_1"}}`)}) {
			return
		}
		for _, frame := range e.frames {
			if !send(cliproxyexecutor.StreamChunk{Payload: []byte(frame)}) {
				return
			}
		}
		if e.release != nil {
			select {
			case <-e.release:
			case <-ctx.Done():
				return
			}
		}
		if e.err != nil {
			send(cliproxyexecutor.StreamChunk{Err: e.err})
		}
	}()
	return &cliproxyexecutor.StreamResult{Chunks: out}, nil
}

func (*streamQuotaExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (*streamQuotaExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*streamQuotaExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

// newStreamQuotaManager registers authCount credentials for a per-test model.
func newStreamQuotaManager(t *testing.T, mode string, executor *streamQuotaExecutor, authCount int) (*Manager, string, []string) {
	t.Helper()
	SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { SetQuotaCooldownResponseMode("") })
	m := NewManager(nil, nil, nil)
	m.RegisterExecutor(executor)
	model := "usage-limit-stream-" + strings.ReplaceAll(t.Name(), "/", "-")
	var ids []string
	for i := 0; i < authCount; i++ {
		id := model + "-" + string(rune('a'+i))
		if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: streamQuotaProvider, Status: StatusActive}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, streamQuotaProvider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		ids = append(ids, id)
	}
	return m, model, ids
}

type streamOutcome struct {
	payloads []string
	err      error
}

func drainStream(t *testing.T, m *Manager, model string) streamOutcome {
	t.Helper()
	result, err := m.ExecuteStream(context.Background(), []string{streamQuotaProvider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v, want a started stream", err)
	}
	var outcome streamOutcome
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			outcome.err = chunk.Err
			continue
		}
		outcome.payloads = append(outcome.payloads, string(chunk.Payload))
	}
	return outcome
}

// A quota failure after output has started becomes the usage limit when the
// whole pool is out, and the upstream's own failure frame is dropped so the
// client sees one terminal error.
func TestStreamUsageLimitReplacesCommittedQuotaFailure(t *testing.T) {
	for _, frames := range [][]string{nil, {codexDuplexQuotaFrame}} {
		name := "error-chunk-only"
		if len(frames) > 0 {
			name = "forwarded-upstream-frame"
		}
		t.Run(name, func(t *testing.T) {
			executor := &streamQuotaExecutor{frames: frames, err: poolQuotaErr{}}
			m, model, ids := newStreamQuotaManager(t, "usage-limit", executor, 1)
			outcome := drainStream(t, m, model)
			resetAt, ok := UsageLimitResetAt(outcome.err)
			if !ok {
				t.Fatalf("stream error = %v, want a usage-limit cooldown", outcome.err)
			}
			if wait := time.Until(resetAt); wait < 2*time.Hour-time.Minute || wait > 2*time.Hour+time.Minute {
				t.Fatalf("reset in %v, want about 2h", wait)
			}
			if len(outcome.payloads) != 1 || !strings.Contains(outcome.payloads[0], "response.created") {
				t.Fatalf("payloads = %q, want only response.created", outcome.payloads)
			}
			// The failure was recorded once, as quota evidence, by the
			// stream's own result tracking.
			auth, _ := m.GetByID(ids[0])
			if window, ok := quotaExhaustionEvidence(auth, model, time.Now()); !ok || window != 0 {
				t.Fatalf("quota evidence = (%d, %v), want recorded with unknown window", window, ok)
			}
		})
	}
}

// Without a qualifying pool, and in retryable mode, the stream is unchanged:
// the upstream frame is forwarded and the raw error ends the stream.
func TestStreamUsageLimitKeepsNonQualifyingStream(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		authCount int
	}{
		{name: "retryable", mode: "retryable", authCount: 1},
		{name: "usage-limit-other-credential-available", mode: "usage-limit", authCount: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := &streamQuotaExecutor{frames: []string{codexDuplexQuotaFrame}, err: poolQuotaErr{}}
			m, model, _ := newStreamQuotaManager(t, tc.mode, executor, tc.authCount)
			outcome := drainStream(t, m, model)
			if IsUsageLimitReachedError(outcome.err) {
				t.Fatalf("stream error = %v, want the raw upstream error", outcome.err)
			}
			if !errors.As(outcome.err, new(poolQuotaErr)) {
				t.Fatalf("stream error = %v, want poolQuotaErr", outcome.err)
			}
			if len(outcome.payloads) != 2 || outcome.payloads[1] != codexDuplexQuotaFrame {
				t.Fatalf("payloads = %q, want response.created then the upstream frame", outcome.payloads)
			}
		})
	}
}

// Only quota failure frames are held for the error chunk that follows them. Any
// other frame, including a recoverable error event, is forwarded at once.
func TestStreamUsageLimitDoesNotDelayOtherFrames(t *testing.T) {
	businessError := `{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"bad tool output"}}`
	executor := &streamQuotaExecutor{frames: []string{businessError}, release: make(chan struct{})}
	m, model, _ := newStreamQuotaManager(t, "usage-limit", executor, 1)
	result, err := m.ExecuteStream(context.Background(), []string{streamQuotaProvider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	defer close(executor.release)
	for _, want := range []string{"response.created", businessError} {
		select {
		case chunk := <-result.Chunks:
			if !strings.Contains(string(chunk.Payload), want) {
				t.Fatalf("payload = %q, want %q", chunk.Payload, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("frame %q was held back", want)
		}
	}
}

func TestStreamPayloadIsQuotaFailure(t *testing.T) {
	cases := map[string]bool{
		codexDuplexQuotaFrame: true,
		// The Codex WebSocket error shape with the error nested under body.
		`{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"quota","resets_in_seconds":7200}}}`: true,
		`{"type":"error","status_code":429,"body":{"error":{"type":"usage_limit_reached","message":"quota"}}}`:                     true,
		`{"type":"response.failed","response":{"error":{"type":"usage_limit_reached","message":"x"}}}`:                             true,
		`{"error":{"type":"insufficient_quota","message":"x"}}`:                                                                    true,
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"x\"}}\n\n":                 true,
		`{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"x"}}`:                                                       true,
		`{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"x"}}`:                                     false,
		`{"type":"response.output_text.delta","delta":"usage_limit_reached"}`:                                                      false,
		`{"choices":[{"delta":{"content":"x"}}],"error":{"type":"rate_limit_exceeded"}}`:                                           false,
		"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"x\"}}\n\n":                 false,
		``: false,
		// An explicit non-429 status wins over the error type, as in the
		// Codex executors: these are recoverable, not quota failures.
		`{"type":"error","status":400,"error":{"type":"rate_limit_error","message":"correct this request"}}`:   false,
		`{"type":"error","status_code":503,"error":{"type":"insufficient_quota","message":"x"}}`:               false,
		`{"type":"response.failed","response":{"error":{"type":"rate_limit_error","status_code":400}}}`:        false,
		`{"type":"error","error":{"type":"usage_limit_reached","status":400,"message":"x"}}`:                   false,
		`{"type":"error","status":429,"error":{"type":"invalid_request_error","message":"too many requests"}}`: true,
		`{"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded","message":"slow down"}}}`: true,
		// Numeric strings count as statuses, as ojson Int() reads them in the
		// executors.
		`{"type":"error","status":"400","error":{"type":"rate_limit_error","message":"correct request"}}`:    false,
		`{"type":"error","status_code":"503","error":{"type":"insufficient_quota","message":"x"}}`:           false,
		`{"type":"error","error":{"type":"rate_limit_error","status":"400","message":"x"}}`:                  false,
		`{"type":"response.failed","response":{"error":{"type":"usage_limit_reached","status_code":"400"}}}`: false,
		`{"type":"error","status":"429","error":{"type":"invalid_request_error","message":"x"}}`:             true,
		`{"type":"error","error":{"type":"invalid_request_error","status_code":"429","message":"x"}}`:        true,
	}
	for payload, want := range cases {
		if got := streamPayloadIsQuotaFailure([]byte(payload)); got != want {
			t.Errorf("streamPayloadIsQuotaFailure(%q) = %v, want %v", payload, got, want)
		}
	}
}

// An executor can coalesce several SSE events into one chunk. When the usage
// limit replaces the quota event, the other events in that chunk are kept.
func TestStreamUsageLimitKeepsOutputCoalescedWithQuotaFrame(t *testing.T) {
	delta := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"LAST_REAL_OUTPUT\"}\n\n"
	payload := delta + "event: error\ndata: " + codexDuplexQuotaFrame + "\n\n"
	m, model, _ := newStreamQuotaManager(t, "usage-limit", &streamQuotaExecutor{frames: []string{payload}, err: poolQuotaErr{}}, 1)
	outcome := drainStream(t, m, model)
	if !IsUsageLimitReachedError(outcome.err) {
		t.Fatalf("stream error = %v, want a usage-limit cooldown", outcome.err)
	}
	if len(outcome.payloads) != 2 || outcome.payloads[1] != delta {
		t.Fatalf("payloads = %q, want response.created then only the output delta", outcome.payloads)
	}
}

// A held quota frame that is not followed by an error chunk is sent unchanged,
// in order.
func TestStreamUsageLimitFlushesHeldFrameWithoutError(t *testing.T) {
	success := `{"type":"response.completed","response":{"id":"resp_1","output":[]}}`
	m, model, _ := newStreamQuotaManager(t, "usage-limit", &streamQuotaExecutor{frames: []string{codexDuplexQuotaFrame, success}}, 1)
	outcome := drainStream(t, m, model)
	if outcome.err != nil || len(outcome.payloads) != 3 || outcome.payloads[1] != codexDuplexQuotaFrame || outcome.payloads[2] != success {
		t.Fatalf("outcome = %+v, want response.created, the frame, then response.completed", outcome)
	}
}

// Canceling the request while a frame is held closes the stream.
func TestStreamUsageLimitClosesOnCancelWhileHolding(t *testing.T) {
	executor := &streamQuotaExecutor{frames: []string{codexDuplexQuotaFrame}, release: make(chan struct{})}
	m, model, _ := newStreamQuotaManager(t, "usage-limit", executor, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := m.ExecuteStream(ctx, []string{streamQuotaProvider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	<-result.Chunks
	cancel()
	for {
		select {
		case _, ok := <-result.Chunks:
			if !ok {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("stream did not close on cancellation")
		}
	}
}
