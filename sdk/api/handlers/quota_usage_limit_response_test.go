package handlers_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/tidwall/gjson"
)

const quotaFakeProvider = "quota-usage-limit-fake"

// quotaRefusal is the upstream 429 the fake executor returns for a credential.
type quotaRefusal struct {
	body       string
	retryAfter *time.Duration
	exhausted  bool
	window     int64
	headers    http.Header
	quotaReset *time.Duration
}

func (e quotaRefusal) Error() string              { return e.body }
func (e quotaRefusal) StatusCode() int            { return http.StatusTooManyRequests }
func (e quotaRefusal) RetryAfter() *time.Duration { return e.retryAfter }
func (e quotaRefusal) IsQuotaExhausted() bool     { return e.exhausted }
func (e quotaRefusal) QuotaLimitWindowMinutes() int64 {
	return e.window
}
func (e quotaRefusal) IsCredentialScoped() bool        { return true }
func (e quotaRefusal) QuotaResetAfter() *time.Duration { return e.quotaReset }
func (e quotaRefusal) Headers() http.Header            { return e.headers.Clone() }

func durationPtr(d time.Duration) *time.Duration { return &d }

const codexUsageLimitBody = `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":7200}}`

// codexUsageLimit is an explicit quota exhaustion with a provider reset.
func codexUsageLimit(reset time.Duration) quotaRefusal {
	return quotaRefusal{
		body:       codexUsageLimitBody,
		retryAfter: durationPtr(reset),
		exhausted:  true,
		window:     300,
		headers:    http.Header{"Retry-After": {strconv.Itoa(int(reset.Seconds()))}},
	}
}

// genericRateLimit is an unclassified 429, even with a long provider hint.
func genericRateLimit(reset time.Duration) quotaRefusal {
	return quotaRefusal{
		body:       `{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}`,
		retryAfter: durationPtr(reset),
		headers:    http.Header{"Retry-After": {strconv.Itoa(int(reset.Seconds()))}},
	}
}

type quotaFakeExecutor struct {
	mu       sync.Mutex
	refusals map[string]quotaRefusal
	calls    atomic.Int32
}

func (*quotaFakeExecutor) Identifier() string { return quotaFakeProvider }

func (e *quotaFakeExecutor) refusalFor(auth *coreauth.Auth) error {
	e.calls.Add(1)
	e.mu.Lock()
	defer e.mu.Unlock()
	if refusal, ok := e.refusals[auth.ID]; ok {
		return refusal
	}
	return errors.New("unexpected credential " + auth.ID)
}

func (e *quotaFakeExecutor) Execute(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.refusalFor(auth)
}

func (e *quotaFakeExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, e.refusalFor(auth)
}

func (*quotaFakeExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*quotaFakeExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*quotaFakeExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type quotaSurface struct {
	name   string
	path   string
	body   func(model string, stream bool) string
	claude bool
}

var quotaSurfaces = []quotaSurface{
	{name: "responses", path: "/v1/responses", body: func(model string, stream bool) string {
		return fmt.Sprintf(`{"model":%q,"input":"hi","stream":%t}`, model, stream)
	}},
	{name: "chat", path: "/v1/chat/completions", body: func(model string, stream bool) string {
		return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":%t}`, model, stream)
	}},
	{name: "messages", path: "/v1/messages", claude: true, body: func(model string, stream bool) string {
		return fmt.Sprintf(`{"model":%q,"max_tokens":16,"messages":[{"role":"user","content":"hi"}],"stream":%t}`, model, stream)
	}},
}

type quotaHarness struct {
	router   *gin.Engine
	executor *quotaFakeExecutor
	model    string
}

func newQuotaHarness(t *testing.T, mode string, refusals []quotaRefusal) *quotaHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	executor := &quotaFakeExecutor{refusals: make(map[string]quotaRefusal)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	model := "quota-model-" + strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
	for i, refusal := range refusals {
		authID := fmt.Sprintf("%s-auth-%d", model, i)
		executor.refusals[authID] = refusal
		if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: authID, Provider: quotaFakeProvider, Status: coreauth.StatusActive}); err != nil {
			t.Fatalf("register %s: %v", authID, err)
		}
		registry.GetGlobalRegistry().RegisterClient(authID, quotaFakeProvider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}

	// Passthrough is on so the test proves an upstream Retry-After is dropped.
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{PassthroughHeaders: true}, manager)
	router := gin.New()
	router.POST("/v1/responses", openai.NewOpenAIResponsesAPIHandler(base).Responses)
	router.POST("/v1/chat/completions", openai.NewOpenAIAPIHandler(base).ChatCompletions)
	router.POST("/v1/messages", claude.NewClaudeCodeAPIHandler(base).ClaudeMessages)
	return &quotaHarness{router: router, executor: executor, model: model}
}

func (h *quotaHarness) do(surface quotaSurface, stream bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, surface.path, strings.NewReader(surface.body(h.model, stream)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, request)
	return recorder
}

var claudeResetSuffix = regexp.MustCompile(`\(reset after (\d+h)?(\d+m)?\d+s\)$`)

func assertUsageLimitResponse(t *testing.T, surface quotaSurface, recorder *httptest.ResponseRecorder, reset time.Duration, window int64) {
	t.Helper()
	body := recorder.Body.Bytes()
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, body)
	}
	if got := recorder.Header().Get("X-Should-Retry"); got != "false" {
		t.Fatalf("X-Should-Retry = %q, want false; body=%s", got, body)
	}
	if got := recorder.Header().Values("Retry-After"); len(got) != 0 {
		t.Fatalf("Retry-After = %q, want absent", got)
	}
	wantReset := time.Now().Add(reset).Unix()
	var resetsAt int64
	if surface.claude {
		if got := gjson.GetBytes(body, "type").String(); got != "error" {
			t.Fatalf("type = %q, want error; body=%s", got, body)
		}
		if got := gjson.GetBytes(body, "error.type").String(); got != "rate_limit_error" {
			t.Fatalf("error.type = %q, want rate_limit_error; body=%s", got, body)
		}
		if message := gjson.GetBytes(body, "error.message").String(); !claudeResetSuffix.MatchString(message) {
			t.Fatalf("error.message = %q, want a (reset after <duration>) suffix", message)
		}
		resetsAt = gjson.GetBytes(body, "error.resets_at").Int()
		if retryable := gjson.GetBytes(body, "error.retryable"); !retryable.Exists() || retryable.Bool() {
			t.Fatalf("error.retryable = %s, want false; body=%s", retryable.Raw, body)
		}
	} else {
		if got := gjson.GetBytes(body, "error.type").String(); got != "usage_limit_reached" {
			t.Fatalf("error.type = %q, want usage_limit_reached; body=%s", got, body)
		}
		if got := gjson.GetBytes(body, "error.code").String(); got != "model_cooldown" {
			t.Fatalf("error.code = %q, want model_cooldown kept; body=%s", got, body)
		}
		if !gjson.GetBytes(body, "error.reset_seconds").Exists() || !gjson.GetBytes(body, "error.reset_time").Exists() {
			t.Fatalf("existing cooldown fields missing; body=%s", body)
		}
		resetsAt = gjson.GetBytes(body, "error.resets_at").Int()
		if retryable := gjson.GetBytes(body, "error.retryable"); !retryable.Exists() || retryable.Bool() {
			t.Fatalf("error.retryable = %s, want false; body=%s", retryable.Raw, body)
		}
		if window > 0 {
			if got := gjson.GetBytes(body, "error.limit_window_minutes").Int(); got != window {
				t.Fatalf("error.limit_window_minutes = %d, want %d; body=%s", got, window, body)
			}
		}
	}
	if diff := resetsAt - wantReset; diff < -60 || diff > 60 {
		t.Fatalf("resets_at = %d, want about %d; body=%s", resetsAt, wantReset, body)
	}
	if window > 0 {
		if got := recorder.Header().Get("X-Codex-Primary-Used-Percent"); got != "100" {
			t.Fatalf("X-Codex-Primary-Used-Percent = %q, want 100", got)
		}
		if got := recorder.Header().Get("X-Codex-Primary-Window-Minutes"); got != strconv.FormatInt(window, 10) {
			t.Fatalf("X-Codex-Primary-Window-Minutes = %q, want %d", got, window)
		}
		if got := recorder.Header().Get("X-Codex-Primary-Reset-At"); got != strconv.FormatInt(resetsAt, 10) {
			t.Fatalf("X-Codex-Primary-Reset-At = %q, want %d", got, resetsAt)
		}
	}
}

func assertRetryableResponse(t *testing.T, surface quotaSurface, recorder *httptest.ResponseRecorder, alreadyCooling bool) {
	t.Helper()
	body := recorder.Body.Bytes()
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, body)
	}
	if got := recorder.Header().Get("X-Should-Retry"); got != "" {
		t.Fatalf("X-Should-Retry = %q, want absent; body=%s", got, body)
	}
	if got := recorder.Header().Get("X-Codex-Primary-Used-Percent"); got != "" {
		t.Fatalf("X-Codex-Primary-Used-Percent = %q, want absent", got)
	}
	if gjson.GetBytes(body, "error.resets_at").Exists() && gjson.GetBytes(body, "error.code").String() == "model_cooldown" {
		t.Fatalf("retryable cooldown reported a usage limit; body=%s", body)
	}
	if surface.claude && claudeResetSuffix.MatchString(gjson.GetBytes(body, "error.message").String()) {
		t.Fatalf("retryable Claude error got a reset suffix; body=%s", body)
	}
	if alreadyCooling {
		// Path (a): today's cooldown answer with Retry-After.
		if seconds, err := strconv.Atoi(recorder.Header().Get("Retry-After")); err != nil || seconds <= 0 {
			t.Fatalf("Retry-After = %q, want a positive delay; body=%s", recorder.Header().Get("Retry-After"), body)
		}
		if !surface.claude {
			if got := gjson.GetBytes(body, "error.code").String(); got != "model_cooldown" {
				t.Fatalf("error.code = %q, want model_cooldown; body=%s", got, body)
			}
			if gjson.GetBytes(body, "error.type").Exists() {
				t.Fatalf("retryable cooldown body gained error.type; body=%s", body)
			}
		}
		return
	}
	// Path (b): the last upstream refusal is passed through, as today.
	if got := gjson.GetBytes(body, "error.code").String(); got == "model_cooldown" {
		t.Fatalf("path (b) replaced the upstream refusal with a cooldown; body=%s", body)
	}
}

// TestQuotaCooldownResponseModeHTTPSurfaces covers both response modes, the
// two pre-commit paths (a: every credential already cooling at selection,
// b: the last credential refuses during this request) and the three HTTP
// surfaces, streaming and not.
func TestQuotaCooldownResponseModeHTTPSurfaces(t *testing.T) {
	const reset = 2 * time.Hour
	for _, mode := range []string{"retryable", "usage-limit"} {
		for _, alreadyCooling := range []bool{false, true} {
			path := "b_last_credential_refuses"
			if alreadyCooling {
				path = "a_all_cooling_at_selection"
			}
			for _, surface := range quotaSurfaces {
				for _, stream := range []bool{false, true} {
					name := fmt.Sprintf("%s/%s/%s/stream=%t", mode, path, surface.name, stream)
					t.Run(name, func(t *testing.T) {
						h := newQuotaHarness(t, mode, []quotaRefusal{codexUsageLimit(reset), codexUsageLimit(reset)})
						if alreadyCooling {
							// The first request walks both credentials into cooldown.
							_ = h.do(surface, stream)
							calls := h.executor.calls.Load()
							recorder := h.do(surface, stream)
							if got := h.executor.calls.Load(); got != calls {
								t.Fatalf("path (a) reached the upstream %d more times", got-calls)
							}
							if mode == "usage-limit" {
								assertUsageLimitResponse(t, surface, recorder, reset, 300)
							} else {
								assertRetryableResponse(t, surface, recorder, true)
							}
							return
						}
						recorder := h.do(surface, stream)
						if got := h.executor.calls.Load(); got != 2 {
							t.Fatalf("upstream calls = %d, want 2 (both credentials tried)", got)
						}
						if mode == "usage-limit" {
							assertUsageLimitResponse(t, surface, recorder, reset, 300)
						} else {
							assertRetryableResponse(t, surface, recorder, false)
						}
					})
				}
			}
		}
	}
}

// TestQuotaCooldownUsageLimitModeKeepsRetryableUnlessEveryCandidateQualifies
// covers pools that must keep today's answer in the usage-limit mode.
func TestQuotaCooldownUsageLimitModeKeepsRetryableUnlessEveryCandidateQualifies(t *testing.T) {
	missingReset := codexUsageLimit(0)
	missingReset.retryAfter = nil
	missingReset.headers = nil
	for _, tc := range []struct {
		name     string
		refusals []quotaRefusal
	}{
		{name: "mixed pool", refusals: []quotaRefusal{codexUsageLimit(2 * time.Hour), genericRateLimit(2 * time.Hour)}},
		{name: "generic long rate limit", refusals: []quotaRefusal{genericRateLimit(2 * time.Hour)}},
		{name: "reset under five minutes", refusals: []quotaRefusal{codexUsageLimit(4 * time.Minute)}},
		{name: "missing reset", refusals: []quotaRefusal{missingReset}},
	} {
		for _, surface := range quotaSurfaces {
			t.Run(tc.name+"/"+surface.name, func(t *testing.T) {
				h := newQuotaHarness(t, "usage-limit", tc.refusals)
				assertRetryableResponse(t, surface, h.do(surface, true), false)
				if tc.name == "missing reset" {
					// Backoff after a 429 without a reset may already have
					// expired; the selection answer is not checked.
					return
				}
				assertRetryableResponse(t, surface, h.do(surface, true), true)
			})
		}
	}
}

// TestQuotaCooldownResponseModeSwitchAppliesToExistingCooldowns shows the mode
// is read per response: evidence recorded under retryable is reported as a
// usage limit right after switching, and switching back restores Retry-After.
func TestQuotaCooldownResponseModeSwitchAppliesToExistingCooldowns(t *testing.T) {
	surface := quotaSurfaces[0]
	h := newQuotaHarness(t, "retryable", []quotaRefusal{codexUsageLimit(2 * time.Hour)})
	assertRetryableResponse(t, surface, h.do(surface, true), false)
	assertRetryableResponse(t, surface, h.do(surface, true), true)
	coreauth.SetQuotaCooldownResponseMode("usage-limit")
	assertUsageLimitResponse(t, surface, h.do(surface, true), 2*time.Hour, 300)
	coreauth.SetQuotaCooldownResponseMode("retryable")
	assertRetryableResponse(t, surface, h.do(surface, true), true)
}

// googleQuotaExhausted mirrors a Gemini or Vertex QUOTA_EXHAUSTED 429: explicit
// evidence and a reset, but no retry hint on the error.
func googleQuotaExhausted(reset time.Duration) quotaRefusal {
	return quotaRefusal{
		body:       `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Quota exhausted"}}`,
		exhausted:  true,
		quotaReset: durationPtr(reset),
	}
}

func TestQuotaResetWithoutRetryHintReportsUsageLimitOnlyInUsageLimitMode(t *testing.T) {
	for _, surface := range quotaSurfaces {
		t.Run("usage-limit/"+surface.name, func(t *testing.T) {
			h := newQuotaHarness(t, "usage-limit", []quotaRefusal{googleQuotaExhausted(2 * time.Hour)})
			assertUsageLimitResponse(t, surface, h.do(surface, true), 2*time.Hour, 0)
			assertUsageLimitResponse(t, surface, h.do(surface, true), 2*time.Hour, 0)
		})
		t.Run("retryable/"+surface.name, func(t *testing.T) {
			h := newQuotaHarness(t, "retryable", []quotaRefusal{googleQuotaExhausted(2 * time.Hour)})
			recorder := h.do(surface, true)
			assertRetryableResponse(t, surface, recorder, false)
			if got := recorder.Header().Get("Retry-After"); got != "" {
				t.Fatalf("default mode gained Retry-After = %q", got)
			}
		})
	}
}
