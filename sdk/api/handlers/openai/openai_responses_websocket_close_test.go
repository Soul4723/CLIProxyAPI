package openai

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// barrierConn blocks the write that carries marker until the connection is
// closed, the way a write to a client that stopped reading blocks, and counts
// Close calls.
type barrierConn struct {
	net.Conn
	marker    []byte
	blocked   chan struct{}
	closed    chan struct{}
	blockOnce sync.Once
	closeOnce sync.Once
	closes    atomic.Int32
}

func (c *barrierConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, c.marker) {
		c.blockOnce.Do(func() { close(c.blocked) })
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *barrierConn) Close() error {
	c.closes.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type barrierListener struct {
	net.Listener
	conns chan *barrierConn
}

func (l *barrierListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &barrierConn{Conn: conn, marker: []byte("usage_limit_reached"), blocked: make(chan struct{}), closed: make(chan struct{})}
	l.conns <- wrapped
	return wrapped, nil
}

// disconnectingCodexExecutor is a codex executor whose upstream disconnect
// notifications the test sends.
type disconnectingCodexExecutor struct {
	usageLimitWebsocketExecutor
	disconnect chan error
}

func (disconnectingCodexExecutor) Identifier() string { return "codex" }

func (e disconnectingCodexExecutor) UpstreamDisconnectChan(string) <-chan error { return e.disconnect }

// When the terminal usage-limit write blocks, a canceled request or an upstream
// disconnect must still close the transport, even though the blocked write has
// already claimed terminal closure, so the handler can exit.
func TestResponsesWebsocketCloseReleasesBlockedTerminalWrite(t *testing.T) {
	for _, trigger := range []string{"cancel", "upstream-disconnect"} {
		t.Run(trigger, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			coreauth.SetQuotaCooldownResponseMode("usage-limit")
			t.Cleanup(func() { coreauth.SetQuotaCooldownResponseMode("") })

			executor := disconnectingCodexExecutor{disconnect: make(chan error, 1)}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			model := "usage-limit-ws-close-" + uuid.NewString()
			authID := model + "-auth"
			if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{ID: authID, Provider: "codex", Status: coreauth.StatusActive}); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
			// Every credential is already out of quota, so the turn ends with
			// the usage-limit event before any upstream work.
			retry := 3 * time.Hour
			manager.MarkResult(context.Background(), coreauth.Result{
				AuthID:                  authID,
				Provider:                "codex",
				Model:                   model,
				Error:                   &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: websocketQuotaErr{exhausted: true}.Error()},
				RetryAfter:              &retry,
				CredentialScope:         true,
				QuotaExhausted:          true,
				QuotaLimitWindowMinutes: 300,
			})

			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			handlerDone := make(chan struct{})
			cancels := make(chan context.CancelFunc, 1)
			router := gin.New()
			router.GET("/v1/responses", func(c *gin.Context) {
				defer close(handlerDone)
				ctx, cancel := context.WithCancel(c.Request.Context())
				defer cancel()
				cancels <- cancel
				c.Request = c.Request.WithContext(ctx)
				h.ResponsesWebsocket(c)
			})
			server := httptest.NewUnstartedServer(router)
			listener := &barrierListener{Listener: server.Listener, conns: make(chan *barrierConn, 1)}
			server.Listener = listener
			server.Start()
			t.Cleanup(server.Close)

			client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			if errDial != nil {
				t.Fatalf("dial websocket: %v", errDial)
			}
			t.Cleanup(func() { _ = client.Close() })
			conn := <-listener.conns
			cancel := <-cancels
			if errWrite := client.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); errWrite != nil {
				t.Fatalf("write create: %v", errWrite)
			}
			select {
			case <-conn.blocked:
			case <-time.After(5 * time.Second):
				t.Fatal("the usage-limit event was never written")
			}

			switch trigger {
			case "cancel":
				cancel()
			case "upstream-disconnect":
				executor.disconnect <- websocketQuotaErr{exhausted: true}
			}
			select {
			case <-handlerDone:
			case <-time.After(3 * time.Second):
				t.Fatalf("handler still blocked 3s after %s; transport Close calls = %d", trigger, conn.closes.Load())
			}
			if conn.closes.Load() == 0 {
				t.Fatal("the transport was never closed")
			}
		})
	}
}
