package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// baseCorsExposedResponseHeaders is the expose list the server sent before the
// quota response mode existed. Retryable mode must keep it byte for byte.
var baseCorsExposedResponseHeaders = []string{
	logging.CPATraceIDHeader,
	"X-CPA-VERSION",
	"X-CPA-COMMIT",
	"X-CPA-BUILD-DATE",
	"X-CPA-SUPPORT-PLUGIN",
	"X-CPA-HOME-VERSION",
	"X-CPA-HOME-BUILD-DATE",
	"X-SERVER-VERSION",
	"X-SERVER-BUILD-DATE",
	"Location",
	"Retry-After",
	"ETag",
	"X-Request-Id",
	"OpenAI-Request-Id",
}

func corsExposeHeaderFor(t *testing.T, mode string) string {
	t.Helper()
	auth.SetQuotaCooldownResponseMode(mode)
	t.Cleanup(func() { auth.SetQuotaCooldownResponseMode("") })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(corsMiddleware())
	engine.GET("/probe", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	req.Header.Set("Origin", "http://127.0.0.1:5173")
	rr := httptest.NewRecorder()
	engine.ServeHTTP(rr, req)
	values := rr.Header().Values("Access-Control-Expose-Headers")
	if len(values) != 1 {
		t.Fatalf("Access-Control-Expose-Headers values = %q, want exactly one", values)
	}
	return values[0]
}

func TestCorsExposeHeadersRetryableModeMatchesBase(t *testing.T) {
	want := strings.Join(baseCorsExposedResponseHeaders, ", ")
	for _, mode := range []string{"", "retryable"} {
		if got := corsExposeHeaderFor(t, mode); got != want {
			t.Fatalf("mode %q: Access-Control-Expose-Headers =\n%q\nwant\n%q", mode, got, want)
		}
	}
}

func TestCorsExposeHeadersUsageLimitModeAddsQuotaHeaders(t *testing.T) {
	got := corsExposeHeaderFor(t, "usage-limit")
	want := strings.Join(append(append([]string(nil), baseCorsExposedResponseHeaders...),
		"X-Should-Retry",
		"X-Codex-Primary-Used-Percent",
		"X-Codex-Primary-Window-Minutes",
		"X-Codex-Primary-Reset-At",
		"X-Codex-Secondary-Used-Percent",
		"X-Codex-Secondary-Window-Minutes",
		"X-Codex-Secondary-Reset-At",
	), ", ")
	if got != want {
		t.Fatalf("Access-Control-Expose-Headers =\n%q\nwant\n%q", got, want)
	}
}
