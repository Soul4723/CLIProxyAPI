package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// The WebSocket tests below are ported from router-for-me/CLIProxyAPI#7184 and
// adapted to the quota-response-mode toggle.

// newUsageLimitError returns the final usage-limit error the manager reports
// when its only credential is out of quota for three hours in a 300-minute
// window, and when that credential reopens.
func newUsageLimitError(t *testing.T) (error, time.Time) {
	t.Helper()
	coreauth.SetQuotaCooldownResponseMode("usage-limit")
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })
	ctx := context.Background()
	model := "usage-limit-ws-" + uuid.NewString()
	refusal := websocketQuotaErr{exhausted: true}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(usageLimitWebsocketExecutor{err: refusal})
	auth := &coreauth.Auth{ID: model, Provider: usageLimitWebsocketProvider, Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, usageLimitWebsocketProvider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	retry := 3 * time.Hour
	manager.MarkResult(ctx, coreauth.Result{
		AuthID:                  auth.ID,
		Provider:                usageLimitWebsocketProvider,
		Model:                   model,
		Error:                   &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: refusal.Error()},
		RetryAfter:              &retry,
		CredentialScope:         true,
		QuotaExhausted:          true,
		QuotaLimitWindowMinutes: 300,
	})
	_, errExecute := manager.Execute(ctx, []string{usageLimitWebsocketProvider}, coreexecutor.Request{Model: model}, coreexecutor.Options{})
	resetAt, ok := coreauth.UsageLimitResetAt(errExecute)
	if !ok {
		t.Fatalf("Execute() error = %v, want a final usage limit", errExecute)
	}
	return errExecute, resetAt
}

func TestShouldExposeResponsesUsageLimit(t *testing.T) {
	usageLimit, _ := newUsageLimitError(t)
	if !shouldExposeResponsesUpstreamError(&interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: usageLimit}) {
		t.Fatalf("usage limit is not exposed")
	}
	cooldown := coreauth.NewModelCooldownError("gpt-6-astra", "codex", 3*time.Hour)
	if shouldExposeResponsesUpstreamError(&interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: cooldown}) {
		t.Fatalf("retryable cooldown is exposed")
	}
}

func TestResponsesWebsocketUsageLimitPayloadCarriesCodexHeaders(t *testing.T) {
	usageLimit, resetAt := newUsageLimitError(t)
	withRetryAfter := handlers.ExecutionErrorMessage(usageLimit)
	withRetryAfter.Addon = http.Header{"Retry-After": {"10800"}, "X-Request-Id": {"req-1"}}
	for name, errMsg := range map[string]*interfaces.ErrorMessage{
		"execution":             handlers.ExecutionErrorMessage(usageLimit),
		"wrapped":               {StatusCode: http.StatusTooManyRequests, Error: fmt.Errorf("select credential: %w", usageLimit)},
		"passthrough addon":     withRetryAfter,
		"bare status and error": {StatusCode: http.StatusTooManyRequests, Error: usageLimit},
	} {
		t.Run(name, func(t *testing.T) {
			payload, errBuild := buildResponsesWebsocketErrorPayload(errMsg)
			if errBuild != nil {
				t.Fatalf("buildResponsesWebsocketErrorPayload() error = %v", errBuild)
			}
			if got := ojson.GetBytes(payload, "type").String(); got != "error" {
				t.Fatalf("type = %q, want error: %s", got, payload)
			}
			if got := ojson.GetBytes(payload, "status").Int(); got != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429: %s", got, payload)
			}
			headers := ojson.GetBytes(payload, "headers")
			if got := headers.Get("X-Should-Retry").String(); got != "false" {
				t.Fatalf("headers.X-Should-Retry = %q, want false: %s", got, payload)
			}
			if got := headers.Get("X-Codex-Primary-Used-Percent").String(); got != "100" {
				t.Fatalf("headers.X-Codex-Primary-Used-Percent = %q, want 100: %s", got, payload)
			}
			if got := headers.Get("X-Codex-Primary-Window-Minutes").String(); got != "300" {
				t.Fatalf("headers.X-Codex-Primary-Window-Minutes = %q, want 300: %s", got, payload)
			}
			if got := headers.Get("X-Codex-Primary-Reset-At").String(); got != strconv.FormatInt(resetAt.Unix(), 10) {
				t.Fatalf("headers.X-Codex-Primary-Reset-At = %q, want %d: %s", got, resetAt.Unix(), payload)
			}
			if headers.Get("Retry-After").Exists() {
				t.Fatalf("headers.Retry-After is set: %s", payload)
			}
			if name == "passthrough addon" && headers.Get("X-Request-Id").String() != "req-1" {
				t.Fatalf("passthrough header dropped: %s", payload)
			}
			if got := ojson.GetBytes(payload, "error.type").String(); got != "usage_limit_reached" {
				t.Fatalf("error.type = %q, want usage_limit_reached: %s", got, payload)
			}
			if got := ojson.GetBytes(payload, "error.resets_at").Int(); got != resetAt.Unix() {
				t.Fatalf("error.resets_at = %d, want %d: %s", got, resetAt.Unix(), payload)
			}
			if got := ojson.GetBytes(payload, "error.limit_window_minutes").Int(); got != 300 {
				t.Fatalf("error.limit_window_minutes = %d, want 300: %s", got, payload)
			}
			if retryable := ojson.GetBytes(payload, "error.retryable"); !retryable.Exists() || retryable.Bool() {
				t.Fatalf("error.retryable = %s, want false: %s", retryable.Raw, payload)
			}
		})
	}
}

const usageLimitWebsocketProvider = "usage-limit-ws-fake"

// usageLimitWebsocketExecutor fails every stream with err, after
// response.created when midStream is set.
type usageLimitWebsocketExecutor struct {
	err       error
	midStream bool
}

func (usageLimitWebsocketExecutor) Identifier() string { return usageLimitWebsocketProvider }

func (usageLimitWebsocketExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e usageLimitWebsocketExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	if !e.midStream {
		return nil, e.err
	}
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","status":"in_progress"}}`)}
	chunks <- coreexecutor.StreamChunk{Err: e.err}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (usageLimitWebsocketExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (usageLimitWebsocketExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (usageLimitWebsocketExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

// websocketQuotaErr is an upstream 429; with exhausted it carries quota
// evidence.
type websocketQuotaErr struct{ exhausted bool }

func (e websocketQuotaErr) Error() string {
	if e.exhausted {
		return `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`
	}
	return `{"error":{"type":"rate_limit_exceeded","message":"slow down"}}`
}
func (websocketQuotaErr) StatusCode() int                 { return http.StatusTooManyRequests }
func (websocketQuotaErr) RetryAfter() *time.Duration      { d := 3 * time.Hour; return &d }
func (e websocketQuotaErr) IsQuotaExhausted() bool        { return e.exhausted }
func (websocketQuotaErr) QuotaLimitWindowMinutes() int64  { return 300 }
func (websocketQuotaErr) QuotaResetAfter() *time.Duration { return nil }
func (websocketQuotaErr) IsCredentialScoped() bool        { return true }

type websocketTurn struct {
	mode string
	// exhausted marks the upstream 429 as quota exhaustion.
	exhausted bool
	// preCooled cools every credential before the turn (path a); otherwise
	// the executor refuses during it, after response.created when midStream.
	preCooled bool
	midStream bool
}

// runResponsesWebsocketTurn returns the frames a client receives for one
// response.create against two refusing credentials.
func runResponsesWebsocketTurn(t *testing.T, turn websocketTurn) [][]byte {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(turn.mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })
	ctx := context.Background()
	model := "usage-limit-ws-" + uuid.NewString()
	manager := coreauth.NewManager(nil, nil, nil)
	refusal := websocketQuotaErr{exhausted: turn.exhausted}
	manager.RegisterExecutor(usageLimitWebsocketExecutor{err: refusal, midStream: turn.midStream})
	authCount := 2
	if turn.midStream {
		// A started stream cannot move to another credential, so one
		// credential is the whole pool.
		authCount = 1
	}
	for i := 0; i < authCount; i++ {
		auth := &coreauth.Auth{ID: fmt.Sprintf("%s-%d", model, i), Provider: usageLimitWebsocketProvider, Status: coreauth.StatusActive}
		if _, errRegister := manager.Register(ctx, auth); errRegister != nil {
			t.Fatalf("Register() error = %v", errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, usageLimitWebsocketProvider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
		if turn.preCooled {
			retry := 3 * time.Hour
			manager.MarkResult(ctx, coreauth.Result{
				AuthID:                  auth.ID,
				Provider:                usageLimitWebsocketProvider,
				Model:                   model,
				Error:                   &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: refusal.Error()},
				RetryAfter:              &retry,
				CredentialScope:         true,
				QuotaExhausted:          turn.exhausted,
				QuotaLimitWindowMinutes: 300,
			})
		}
	}

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()
	create := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","role":"user","content":"ping"}]}`, model)
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(create)); errWrite != nil {
		t.Fatalf("write create: %v", errWrite)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var frames [][]byte
	for {
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			var netErr interface{ Timeout() bool }
			if errors.As(errRead, &netErr) && netErr.Timeout() {
				t.Fatalf("websocket stayed open after the refusal; frames=%q", frames)
			}
			return frames
		}
		frames = append(frames, payload)
	}
}

func assertWebsocketUsageLimitEvent(t *testing.T, payload []byte) {
	t.Helper()
	if got := ojson.GetBytes(payload, "type").String(); got != "error" {
		t.Fatalf("type = %q, want error: %s", got, payload)
	}
	if got := ojson.GetBytes(payload, "status").Int(); got != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429: %s", got, payload)
	}
	if got := ojson.GetBytes(payload, "error.type").String(); got != "usage_limit_reached" {
		t.Fatalf("error.type = %q, want usage_limit_reached: %s", got, payload)
	}
	wantReset := time.Now().Add(3 * time.Hour).Unix()
	if got := ojson.GetBytes(payload, "error.resets_at").Int(); got < wantReset-5 || got > wantReset+5 {
		t.Fatalf("error.resets_at = %d, want about %d: %s", got, wantReset, payload)
	}
	if got := ojson.GetBytes(payload, "headers.X-Codex-Primary-Used-Percent").String(); got != "100" {
		t.Fatalf("headers.X-Codex-Primary-Used-Percent = %q, want 100: %s", got, payload)
	}
	if got := ojson.GetBytes(payload, "headers.X-Should-Retry").String(); got != "false" {
		t.Fatalf("headers.X-Should-Retry = %q, want false: %s", got, payload)
	}
}

// When every credential is out of quota, the client gets the usage limit as an
// error event before the socket closes, instead of a silent close.
func TestResponsesWebsocketSendsUsageLimitWhenEveryCredentialIsOut(t *testing.T) {
	for _, preCooled := range []bool{true, false} {
		name := "refused-during-turn"
		if preCooled {
			name = "already-cooling"
		}
		t.Run(name, func(t *testing.T) {
			frames := runResponsesWebsocketTurn(t, websocketTurn{mode: "usage-limit", exhausted: true, preCooled: preCooled})
			if len(frames) != 1 {
				t.Fatalf("frames = %d (%q), want one error event", len(frames), frames)
			}
			assertWebsocketUsageLimitEvent(t, frames[0])
		})
	}
}

// A quota failure after response.created ends with the usage limit, once.
func TestResponsesWebsocketSendsUsageLimitAfterResponseStarted(t *testing.T) {
	frames := runResponsesWebsocketTurn(t, websocketTurn{mode: "usage-limit", exhausted: true, midStream: true})
	if len(frames) != 2 {
		t.Fatalf("frames = %d (%q), want response.created then one error event", len(frames), frames)
	}
	if got := ojson.GetBytes(frames[0], "type").String(); got != "response.created" {
		t.Fatalf("first frame type = %q, want response.created", got)
	}
	assertWebsocketUsageLimitEvent(t, frames[1])
}

// Retryable mode, and a pool that is only rate limited, keep the silent close.
func TestResponsesWebsocketKeepsSilentCloseWithoutUsageLimit(t *testing.T) {
	cases := []struct {
		name string
		turn websocketTurn
		want int
	}{
		{name: "retryable-already-cooling", turn: websocketTurn{mode: "retryable", exhausted: true, preCooled: true}},
		{name: "retryable-refused-during-turn", turn: websocketTurn{mode: "retryable", exhausted: true}},
		{name: "retryable-after-response-started", turn: websocketTurn{mode: "retryable", exhausted: true, midStream: true}, want: 1},
		{name: "usage-limit-rate-limited-already-cooling", turn: websocketTurn{mode: "usage-limit", preCooled: true}},
		{name: "usage-limit-rate-limited-during-turn", turn: websocketTurn{mode: "usage-limit"}},
		{name: "usage-limit-rate-limited-after-response-started", turn: websocketTurn{mode: "usage-limit", midStream: true}, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frames := runResponsesWebsocketTurn(t, tc.turn)
			if len(frames) != tc.want {
				t.Fatalf("frames = %q, want %d frame(s) and then a silent close", frames, tc.want)
			}
			for _, frame := range frames {
				if got := ojson.GetBytes(frame, "type").String(); got != "response.created" {
					t.Fatalf("frame type = %q, want only response.created: %s", got, frame)
				}
			}
		})
	}
}

// The usage limit is decided before the replay-close and silent-close paths: a
// connection-scoped turn that would be replayed on a new socket (1012) still
// gets the usage-limit event, while other errors keep that path.
func TestForwardResponsesWebsocketUsageLimitPrecedesReplayClose(t *testing.T) {
	usageLimit, _ := newUsageLimitError(t)
	cases := []struct {
		name      string
		err       error
		wantEvent bool
	}{
		{name: "usage-limit", err: usageLimit, wantEvent: true},
		{name: "retryable-cooldown", err: coreauth.NewModelCooldownError("gpt-6-astra", "codex", 3*time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			type serverResult struct {
				suppressed bool
				errMsg     *interfaces.ErrorMessage
				errForward error
			}
			resultCh := make(chan serverResult, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					resultCh <- serverResult{errForward: err}
					return
				}
				defer func() { _ = conn.Close() }()
				ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ctx.Request = r
				errCh := make(chan *interfaces.ErrorMessage, 1)
				errCh <- handlers.ExecutionErrorMessage(tc.err)
				suppressed := false
				h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
				_, _, _, errMsg, errForward := h.forwardResponsesWebsocket(
					ctx,
					newResponsesWebsocketWriter(conn),
					func(...interface{}) {},
					make(chan []byte),
					errCh,
					newInMemoryWebsocketTimelineLog(),
					"session-1",
					responsesWebsocketForwardOptions{suppressError: func(*interfaces.ErrorMessage) bool {
						suppressed = true
						return true
					}},
				)
				resultCh <- serverResult{suppressed: suppressed, errMsg: errMsg, errForward: errForward}
				if !tc.wantEvent {
					// The caller closes the socket with 1012 after a
					// suppressed error; close it here instead.
					_ = conn.Close()
				}
			}))
			defer server.Close()

			conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if errDial != nil {
				t.Fatalf("dial websocket: %v", errDial)
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, payload, errRead := conn.ReadMessage()
			result := <-resultCh

			if !tc.wantEvent {
				if errRead == nil {
					t.Fatalf("got frame %s, want none", payload)
				}
				if !result.suppressed || result.errForward != nil {
					t.Fatalf("suppressed = %v, errForward = %v; want the replay path", result.suppressed, result.errForward)
				}
				return
			}
			if errRead != nil {
				t.Fatalf("read usage-limit event: %v", errRead)
			}
			assertWebsocketUsageLimitEvent(t, payload)
			if result.suppressed {
				t.Fatalf("usage limit went to the replay path")
			}
			if !errors.Is(result.errForward, websocket.ErrCloseSent) {
				t.Fatalf("errForward = %v, want %v", result.errForward, websocket.ErrCloseSent)
			}
		})
	}
}
