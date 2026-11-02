package openai

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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
			return frames
		}
		frames = append(frames, string(payload))
	}
}

// With steering off, the Codex executor also reports the failure on its
// upstream disconnect channel, whose subscriber used to close the socket
// before the stream's normalised error arrived. The usage-limit event must
// reach the client before the close, exactly once, for both quota frame
// shapes the executor accepts.
func TestRealCodexWebsocketUsageLimitBeatsDisconnectClose(t *testing.T) {
	for name, failure := range map[string]string{"direct": codexQuotaFrameDirect, "wrapped": codexQuotaFrameWrapped} {
		t.Run(name, func(t *testing.T) {
			frames := runRealCodexWebsocketTurn(t, "usage-limit", false, failure)
			if len(frames) != 1 {
				t.Fatalf("terminal frames = %d (%q), want exactly one usage-limit event", len(frames), frames)
			}
			assertCodexWebsocketUsageLimit(t, frames[0])
		})
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
				frames := runRealCodexWebsocketTurn(t, "retryable", steering, failure)
				if strings.Join(frames, "\n") != strings.Join(want, "\n") {
					t.Fatalf("frames = %q, want %q", frames, want)
				}
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
