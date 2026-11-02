package openai

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// These tests run the real handler, manager and Codex executor against a
// loopback Codex WebSocket upstream that sends response.created and then a
// quota failure.

const (
	codexQuotaFrameDirect  = `{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"quota","resets_in_seconds":7200}}`
	codexQuotaFrameWrapped = `{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"quota","resets_in_seconds":7200}}}`
)

// runRealCodexWebsocketTurn returns the frames the client receives after
// response.created when the upstream answers the turn with failure.
func runRealCodexWebsocketTurn(t *testing.T, mode string, steering bool, failure string) []string {
	frames, _ := runRealCodexWebsocketTurnWithClose(t, mode, steering, failure)
	return frames
}

// runRealCodexWebsocketTurnWithClose also returns the error that ended the
// client's reads, which carries the close code.
func runRealCodexWebsocketTurnWithClose(t *testing.T, mode string, steering bool, failure string) ([]string, error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	releaseFailure := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upstream upgrade: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("upstream read create: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1","output":[]}}`))
		// Fail only after the client has seen response.created.
		<-releaseFailure
		_ = conn.WriteMessage(websocket.TextMessage, []byte(failure))
		_, _, _ = conn.ReadMessage()
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = steering
	cfg.CodexResponseSteering = steering
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	model := "usage-limit-codex-ws-" + uuid.NewString()
	authID := model + "-auth"
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"base_url": upstream.URL, "websockets": "true"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)

	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	create := `{"type":"response.create","model":"` + model + `","input":[]}`
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(create)); errWrite != nil {
		t.Fatalf("write create: %v", errWrite)
	}
	_, first, errRead := conn.ReadMessage()
	if errRead != nil {
		t.Fatalf("read response.created: %v", errRead)
	}
	if got := ojson.GetBytes(first, "type").String(); got != "response.created" {
		t.Fatalf("first frame type = %q, want response.created: %s", got, first)
	}
	close(releaseFailure)
	var frames []string
	for {
		_, payload, errNext := conn.ReadMessage()
		if errNext != nil {
			var netErr net.Error
			if errors.As(errNext, &netErr) && netErr.Timeout() {
				t.Fatalf("websocket stayed open after the failure; frames=%q", frames)
			}
			return frames, errNext
		}
		frames = append(frames, string(payload))
	}
}

// With steering off, the Codex executor reports a quota failure after
// response.created on its upstream disconnect channel, and the socket closes at
// once without an event (1006), in both modes, as on base. The usage limit
// reaches the client on its next request instead; see
// TestRealCodexWebsocketQuotaReachesClientOnReconnect.
func TestRealCodexWebsocketMidStreamQuotaClosesSilently(t *testing.T) {
	for _, mode := range []string{"retryable", "usage-limit"} {
		for name, failure := range map[string]string{"direct": codexQuotaFrameDirect, "wrapped": codexQuotaFrameWrapped} {
			t.Run(mode+"/"+name, func(t *testing.T) {
				frames, closeErr := runRealCodexWebsocketTurnWithClose(t, mode, false, failure)
				if len(frames) != 0 {
					t.Fatalf("frames = %q, want a silent close", frames)
				}
				assertAbnormalClose(t, closeErr)
			})
		}
	}
}

// With steering on, the duplex executor forwards the upstream frame before its
// error chunk. A wrapped frame ({"body":{"error":...}}) must be intercepted
// like a direct one, so the client gets only the usage-limit event.
func TestRealCodexDuplexQuotaFrameSentOnce(t *testing.T) {
	for name, failure := range map[string]string{"direct": codexQuotaFrameDirect, "wrapped": codexQuotaFrameWrapped} {
		t.Run(name, func(t *testing.T) {
			frames := runRealCodexWebsocketTurn(t, "usage-limit", true, failure)
			if len(frames) != 1 {
				t.Fatalf("terminal frames = %d (%q), want exactly one usage-limit event", len(frames), frames)
			}
			assertCodexWebsocketUsageLimit(t, frames[0])
		})
	}
}

// Retryable mode keeps the base behaviour: a silent close without steering,
// and the upstream frame alone with steering.
func TestRealCodexWebsocketRetryableModeUnchanged(t *testing.T) {
	for _, steering := range []bool{false, true} {
		for name, failure := range map[string]string{"direct": codexQuotaFrameDirect, "wrapped": codexQuotaFrameWrapped} {
			label := "steering-off/" + name
			want := []string(nil)
			if steering {
				label = "steering-on/" + name
				want = []string{failure}
			}
			t.Run(label, func(t *testing.T) {
				frames, closeErr := runRealCodexWebsocketTurnWithClose(t, "retryable", steering, failure)
				if strings.Join(frames, "\n") != strings.Join(want, "\n") {
					t.Fatalf("frames = %q, want %q", frames, want)
				}
				assertAbnormalClose(t, closeErr)
			})
		}
	}
}

func assertCodexWebsocketUsageLimit(t *testing.T, payload string) {
	t.Helper()
	if got := ojson.Get(payload, "type").String(); got != "error" {
		t.Fatalf("type = %q, want error: %s", got, payload)
	}
	if got := ojson.Get(payload, "status").Int(); got != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429: %s", got, payload)
	}
	if got := ojson.Get(payload, "error.type").String(); got != "usage_limit_reached" {
		t.Fatalf("error.type = %q, want usage_limit_reached: %s", got, payload)
	}
	wantReset := time.Now().Add(2 * time.Hour).Unix()
	if got := ojson.Get(payload, "error.resets_at").Int(); got < wantReset-5 || got > wantReset+5 {
		t.Fatalf("error.resets_at = %d, want about %d: %s", got, wantReset, payload)
	}
	if retryable := ojson.Get(payload, "error.retryable"); !retryable.Exists() || retryable.Bool() {
		t.Fatalf("error.retryable = %s, want false: %s", retryable.Raw, payload)
	}
	if got := ojson.Get(payload, "headers.X-Should-Retry").String(); got != "false" {
		t.Fatalf("headers.X-Should-Retry = %q, want false: %s", got, payload)
	}
}

// runRealCodexRecoverableError returns the first frame the client receives
// after response.created when the duplex upstream sends a recoverable error
// and then waits for the client's next request.
func runRealCodexRecoverableError(t *testing.T, mode string, failure string) ([]byte, error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	releaseFailure := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upstream upgrade: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("upstream read create: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1","output":[]}}`))
		<-releaseFailure
		_ = conn.WriteMessage(websocket.TextMessage, []byte(failure))
		// Wait for the client's next request, as a duplex upstream does.
		_, _, _ = conn.ReadMessage()
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.CodexResponseSteering = true
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	model := "usage-limit-codex-ws-" + uuid.NewString()
	authID := model + "-auth"
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"base_url": upstream.URL, "websockets": "true"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)

	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"`+model+`","input":[]}`)); errWrite != nil {
		t.Fatalf("write create: %v", errWrite)
	}
	if _, first, errRead := conn.ReadMessage(); errRead != nil || ojson.GetBytes(first, "type").String() != "response.created" {
		t.Fatalf("read response.created: %s, %v", first, errRead)
	}
	close(releaseFailure)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, errRead := conn.ReadMessage()
	return payload, errRead
}

// A duplex error with an explicit status 400 is recoverable even when its type
// names a rate limit. It is forwarded at once in both modes, not held for an
// error chunk that never comes.
func TestRealCodexDuplexRecoverableErrorNotHeld(t *testing.T) {
	failures := map[string]string{
		"numeric-status": `{"type":"error","status":400,"error":{"type":"rate_limit_error","message":"correct this request"}}`,
		// The executor reads "400" as 400 too.
		"string-status": `{"type":"error","status":"400","error":{"type":"rate_limit_error","message":"correct request"}}`,
	}
	for name, failure := range failures {
		for _, mode := range []string{"retryable", "usage-limit"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				payload, errRead := runRealCodexRecoverableError(t, mode, failure)
				if errRead != nil {
					t.Fatalf("recoverable error was not forwarded while the upstream waits: %v", errRead)
				}
				if string(payload) != failure {
					t.Fatalf("frame = %s, want the upstream error unchanged", payload)
				}
			})
		}
	}
}

// blockedTurn is a real Codex WebSocket turn (steering off) whose upstream
// sends a 16 MiB output delta and then a quota failure.
type blockedTurn struct {
	client      *websocket.Conn
	handlerDone chan struct{}
	failureSent chan struct{}
	bigFrame    int
}

func startRealCodexBlockedTurn(t *testing.T, mode string) *blockedTurn {
	t.Helper()
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	turn := &blockedTurn{handlerDone: make(chan struct{}), failureSent: make(chan struct{}), bigFrame: 16 << 20}
	delta := `{"type":"response.output_text.delta","delta":"` + strings.Repeat("x", turn.bigFrame) + `"}`
	releaseOutput := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upstream upgrade: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("upstream read create: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1","output":[]}}`))
		<-releaseOutput
		_ = conn.WriteMessage(websocket.TextMessage, []byte(delta))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(codexQuotaFrameDirect))
		close(turn.failureSent)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, _, _ = conn.ReadMessage()
	}))

	cfg := &config.Config{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	model := "usage-limit-codex-ws-" + uuid.NewString()
	authID := model + "-auth"
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"base_url": upstream.URL, "websockets": "true"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", func(c *gin.Context) {
		defer close(turn.handlerDone)
		h.ResponsesWebsocket(c)
	})
	downstream := httptest.NewServer(router)

	client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	turn.client = client
	// Cleanups run last-in first-out: the client closes before the servers,
	// so a blocked handler can always finish and the servers can shut down.
	t.Cleanup(upstream.Close)
	t.Cleanup(downstream.Close)
	t.Cleanup(func() {
		_ = client.Close()
		<-turn.handlerDone
	})

	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if errWrite := client.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"`+model+`","input":[]}`)); errWrite != nil {
		t.Fatalf("write create: %v", errWrite)
	}
	if _, first, errRead := client.ReadMessage(); errRead != nil || ojson.GetBytes(first, "type").String() != "response.created" {
		t.Fatalf("read response.created: %s, %v", first, errRead)
	}
	close(releaseOutput)
	return turn
}

func (b *blockedTurn) handlerExitsWithin(limit time.Duration) bool {
	select {
	case <-b.handlerDone:
		return true
	case <-time.After(limit):
		return false
	}
}

// A client that stops reading during a large frame must not keep the handler
// alive when the upstream then fails with exhausted quota: in both modes the
// socket closes at once, as on base, and the handler exits.
func TestRealCodexWebsocketBlockedClientDoesNotHangHandler(t *testing.T) {
	for _, mode := range []string{"retryable", "usage-limit"} {
		t.Run(mode, func(t *testing.T) {
			turn := startRealCodexBlockedTurn(t, mode)
			select {
			case <-turn.failureSent:
			case <-time.After(10 * time.Second):
				t.Fatal("upstream did not send the quota failure")
			}
			if !turn.handlerExitsWithin(3 * time.Second) {
				t.Fatal("handler still blocked 3s after the upstream failed")
			}
			_ = turn.client.SetReadDeadline(time.Now().Add(5 * time.Second))
			for {
				_, payload, errRead := turn.client.ReadMessage()
				if errRead != nil {
					assertAbnormalClose(t, errRead)
					return
				}
				if got := ojson.GetBytes(payload, "type").String(); got != "response.output_text.delta" {
					t.Fatalf("frame type = %q, want only output before the silent close", got)
				}
			}
		})
	}
}

// assertAbnormalClose checks that the socket closed without a close frame
// (1006), the way the retryable mode closes on base.
func assertAbnormalClose(t *testing.T, err error) {
	t.Helper()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseAbnormalClosure {
		t.Fatalf("close = %v, want 1006 (abnormal closure)", err)
	}
}

// A quota failure mid-stream closes the socket silently. Codex then reconnects,
// and its next request finds every credential cooling with quota evidence, so
// it gets exactly one usage-limit event, with the reset and the x-codex window
// headers, before the socket closes. The upstream is not contacted again.
func TestRealCodexWebsocketQuotaReachesClientOnReconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	coreauth.SetQuotaCooldownResponseMode("usage-limit")
	t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

	failure := `{"type":"error","status":429,"headers":{"x-codex-primary-used-percent":"100","x-codex-primary-window-minutes":"300"},"error":{"type":"usage_limit_reached","message":"quota","resets_in_seconds":7200}}`
	var upstreamConnections atomic.Int32
	releaseFailure := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamConnections.Add(1)
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upstream upgrade: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("upstream read create: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"r1","output":[]}}`))
		<-releaseFailure
		_ = conn.WriteMessage(websocket.TextMessage, []byte(failure))
		_, _, _ = conn.ReadMessage()
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	model := "usage-limit-codex-ws-" + uuid.NewString()
	authID := model + "-auth"
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"base_url": upstream.URL, "websockets": "true"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	downstream := httptest.NewServer(router)
	t.Cleanup(downstream.Close)
	wsURL := "ws" + strings.TrimPrefix(downstream.URL, "http") + "/v1/responses"
	create := []byte(`{"type":"response.create","model":"` + model + `","input":[]}`)

	// First connection: output starts, then the quota failure closes it.
	first, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = first.Close() }()
	_ = first.SetReadDeadline(time.Now().Add(5 * time.Second))
	if errWrite := first.WriteMessage(websocket.TextMessage, create); errWrite != nil {
		t.Fatalf("write create: %v", errWrite)
	}
	if _, payload, errRead := first.ReadMessage(); errRead != nil || ojson.GetBytes(payload, "type").String() != "response.created" {
		t.Fatalf("read response.created: %s, %v", payload, errRead)
	}
	close(releaseFailure)
	_, payload, errRead := first.ReadMessage()
	if errRead == nil {
		t.Fatalf("first connection got %s after the quota failure, want a silent close", payload)
	}
	assertAbnormalClose(t, errRead)

	// The failure is recorded by the stream's result tracking; wait until the
	// credential is cooling before reconnecting, as a reconnect would find it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if auth, ok := manager.GetByID(authID); ok && auth.Quota.Exceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("credential was not marked as cooling after the quota failure")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Reconnect: the next request gets the usage limit.
	second, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
	if errDial != nil {
		t.Fatalf("redial websocket: %v", errDial)
	}
	defer func() { _ = second.Close() }()
	_ = second.SetReadDeadline(time.Now().Add(5 * time.Second))
	if errWrite := second.WriteMessage(websocket.TextMessage, create); errWrite != nil {
		t.Fatalf("write second create: %v", errWrite)
	}
	var frames []string
	for {
		_, next, errNext := second.ReadMessage()
		if errNext != nil {
			break
		}
		frames = append(frames, string(next))
	}
	if len(frames) != 1 {
		t.Fatalf("reconnect frames = %d (%q), want exactly one usage-limit event", len(frames), frames)
	}
	assertCodexWebsocketUsageLimit(t, frames[0])
	headers := ojson.Get(frames[0], "headers")
	if got := headers.Get("X-Codex-Primary-Used-Percent").String(); got != "100" {
		t.Fatalf("headers.X-Codex-Primary-Used-Percent = %q, want 100: %s", got, frames[0])
	}
	if got := headers.Get("X-Codex-Primary-Window-Minutes").String(); got != "300" {
		t.Fatalf("headers.X-Codex-Primary-Window-Minutes = %q, want 300: %s", got, frames[0])
	}
	wantReset := time.Now().Add(2 * time.Hour).Unix()
	if got := headers.Get("X-Codex-Primary-Reset-At").Int(); got < wantReset-5 || got > wantReset+5 {
		t.Fatalf("headers.X-Codex-Primary-Reset-At = %d, want about %d: %s", got, wantReset, frames[0])
	}
	if headers.Get("Retry-After").Exists() {
		t.Fatalf("headers.Retry-After is set: %s", frames[0])
	}
	if got := upstreamConnections.Load(); got != 1 {
		t.Fatalf("upstream connections = %d, want 1: the reconnect must not reach the upstream", got)
	}
}
