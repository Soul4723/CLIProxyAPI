package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

const vertexOpenAPIQuotaModel = "deepseek-ai/deepseek-v4.1-flash-maas"

func vertexOpenAPIQuotaServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, googleQuotaExhaustedBody)
	}))
	t.Cleanup(server.Close)
	return server
}

func vertexOpenAPIQuotaRun(t *testing.T, manager *cliproxyauth.Manager, stream bool) error {
	t.Helper()
	req := cliproxyexecutor.Request{
		Model:   vertexOpenAPIQuotaModel,
		Payload: []byte(`{"model":"` + vertexOpenAPIQuotaModel + `","messages":[{"role":"user","content":"hi"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAI,
		ResponseFormat: sdktranslator.FormatOpenAI,
		Stream:         stream,
	}
	if !stream {
		_, err := manager.Execute(context.Background(), []string{"vertex"}, req, opts)
		return err
	}
	result, err := manager.ExecuteStream(context.Background(), []string{"vertex"}, req, opts)
	if err != nil {
		return err
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			return chunk.Err
		}
	}
	return nil
}

// TestVertexOpenAPIReportsGoogleQuotaExhaustion routes a Vertex MaaS model
// through the OpenAPI delegation and checks that a Google QUOTA_EXHAUSTED 429
// reaches the manager as quota evidence, for execute and stream.
func TestVertexOpenAPIReportsGoogleQuotaExhaustion(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			server := vertexOpenAPIQuotaServer(t)
			cfg := &config.Config{}
			manager := cliproxyauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.RegisterExecutor(NewGeminiVertexExecutor(cfg))

			authID := fmt.Sprintf("vertex-openapi-quota-%v-%d", stream, time.Now().UnixNano())
			registry.GetGlobalRegistry().RegisterClient(authID, "vertex", []*registry.ModelInfo{{ID: vertexOpenAPIQuotaModel}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
			if _, errRegister := manager.Register(context.Background(), &cliproxyauth.Auth{
				ID:         authID,
				Provider:   "vertex",
				Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL},
			}); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}

			// Default mode: the raw upstream error, its text and retry hint unchanged.
			cliproxyauth.SetQuotaCooldownResponseMode("")
			t.Cleanup(func() { cliproxyauth.SetQuotaCooldownResponseMode("") })
			errDefault := vertexOpenAPIQuotaRun(t, manager, stream)
			if errDefault == nil {
				t.Fatal("default mode: got nil error, want 429")
			}
			if cliproxyauth.IsUsageLimitReachedError(errDefault) {
				t.Fatalf("default mode: got usage limit, want retryable: %v", errDefault)
			}
			var hinted interface{ RetryAfter() *time.Duration }
			// The manager reports the time left until the recorded deadline.
			if !errors.As(errDefault, &hinted) || hinted.RetryAfter() == nil ||
				*hinted.RetryAfter() <= 7100*time.Second || *hinted.RetryAfter() > 7200*time.Second {
				t.Fatalf("default mode: retry hint missing or changed: %v", errDefault)
			}
			if errDefault.Error() != googleQuotaExhaustedBody {
				t.Fatalf("default mode: error text changed: %q", errDefault.Error())
			}
			var quota cliproxyexecutor.QuotaExhaustedError
			if !errors.As(errDefault, &quota) || !quota.IsQuotaExhausted() {
				t.Fatalf("default mode: IsQuotaExhausted() = false, want true: %v", errDefault)
			}
			var reset cliproxyexecutor.QuotaResetError
			if !errors.As(errDefault, &reset) || reset.QuotaResetAfter() == nil || *reset.QuotaResetAfter() != 7200*time.Second {
				t.Fatalf("default mode: QuotaResetAfter() missing or wrong: %v", errDefault)
			}

			// Usage-limit mode: the only credential now cools down with
			// evidence, so the manager reports a final usage limit.
			cliproxyauth.SetQuotaCooldownResponseMode("usage-limit")
			errLimit := vertexOpenAPIQuotaRun(t, manager, stream)
			if !cliproxyauth.IsUsageLimitReachedError(errLimit) {
				t.Fatalf("usage-limit mode: got %v, want usage limit", errLimit)
			}
			resetAt, ok := cliproxyauth.UsageLimitResetAt(errLimit)
			if !ok || time.Until(resetAt) < 7100*time.Second || time.Until(resetAt) > 7300*time.Second {
				t.Fatalf("usage-limit mode: resets_at = %v (ok=%v), want about 2h from now", resetAt, ok)
			}
		})
	}
}
