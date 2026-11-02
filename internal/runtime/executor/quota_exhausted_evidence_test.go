package executor

import (
	"errors"
	"net/http"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func quotaEvidenceOf(err error) (bool, int64) {
	quotaErr, ok := errors.AsType[cliproxyexecutor.QuotaExhaustedError](err)
	if !ok || !quotaErr.IsQuotaExhausted() {
		return false, 0
	}
	var window int64
	if windowErr, okWindow := errors.AsType[cliproxyexecutor.QuotaLimitWindowError](err); okWindow {
		window = windowErr.QuotaLimitWindowMinutes()
	}
	return true, window
}

func TestStatusErrorsReportOnlyStructuredQuotaExhaustion(t *testing.T) {
	codexUsageLimit := []byte(`{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":3600}}`)
	codexHeaders := func(primaryUsed, secondaryUsed string) http.Header {
		return http.Header{
			"X-Codex-Primary-Used-Percent":     {primaryUsed},
			"X-Codex-Primary-Window-Minutes":   {"300"},
			"X-Codex-Primary-Reset-At":         {"1793590000"},
			"X-Codex-Secondary-Used-Percent":   {secondaryUsed},
			"X-Codex-Secondary-Window-Minutes": {"10080"},
			"X-Codex-Secondary-Reset-At":       {"1794000000"},
		}
	}
	claudeRejected := func(window string) http.Header {
		return http.Header{
			"Anthropic-Ratelimit-Unified-Status":                {"rejected"},
			"Anthropic-Ratelimit-Unified-" + window + "-Status": {"rejected"},
			"Anthropic-Ratelimit-Unified-Reset":                 {"1793590000"},
		}
	}
	tests := []struct {
		name       string
		err        error
		want       bool
		wantWindow int64
	}{
		{name: "codex usage_limit_reached", err: newCodexStatusErr(http.StatusTooManyRequests, codexUsageLimit), want: true},
		{name: "codex usage_limit_reached primary window", err: newCodexStatusErrWithHeaders(http.StatusTooManyRequests, codexHeaders("100", "40"), codexUsageLimit), want: true, wantWindow: 300},
		{name: "codex usage_limit_reached weekly window", err: newCodexStatusErrWithHeaders(http.StatusTooManyRequests, codexHeaders("100", "100"), codexUsageLimit), want: true, wantWindow: 10080},
		{name: "codex generic rate limit", err: newCodexStatusErr(http.StatusTooManyRequests, []byte(`{"error":{"type":"rate_limit_exceeded","message":"Rate limit reached"}}`))},
		{
			name: "codex websocket usage_limit_reached",
			err: func() error {
				err, _ := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached"}}`))
				return err
			}(),
			want: true,
		},
		{name: "antigravity QUOTA_EXHAUSTED", err: newAntigravityStatusErr(http.StatusTooManyRequests, []byte(`{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Quota exhausted","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}}`)), want: true},
		{name: "antigravity long RATE_LIMIT_EXCEEDED", err: newAntigravityStatusErr(http.StatusTooManyRequests, []byte(`{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Rate limited","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"7200s"}]}}`))},
		{name: "antigravity quota keyword only", err: newAntigravityStatusErr(http.StatusTooManyRequests, []byte(`{"error":{"code":429,"message":"You have exhausted your capacity on this model. Your quota will reset after 2h."}}`))},
		{name: "openai insufficient_quota", err: newOpenAICompatStatusError(http.StatusTooManyRequests, nil, []byte(`{"error":{"message":"You exceeded your current quota","type":"insufficient_quota","code":"insufficient_quota"}}`)), want: true},
		{name: "openai rate_limit_exceeded", err: newOpenAICompatStatusError(http.StatusTooManyRequests, nil, []byte(`{"error":{"message":"Rate limit reached","type":"requests","code":"rate_limit_exceeded"}}`))},
		{name: "openai insufficient_quota on a non-429", err: newOpenAICompatStatusError(http.StatusForbidden, nil, []byte(`{"error":{"type":"insufficient_quota","code":"insufficient_quota"}}`))},
		{name: "claude unified 5h rejection", err: classifyClaudeUpstreamError(http.StatusTooManyRequests, claudeRejected("5h"), []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"limited"}}`)), want: true, wantWindow: 300},
		{name: "claude unified 7d rejection with model-level cooling", err: classifyClaudeUpstreamErrorWithCooling(http.StatusTooManyRequests, claudeRejected("7d"), []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"limited"}}`), true), want: true, wantWindow: 10080},
		{name: "claude ordinary 429", err: classifyClaudeUpstreamError(http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed the requests per minute rate limit. Please try again later."}}`))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.err == nil {
				t.Fatal("constructor returned nil error")
			}
			got, window := quotaEvidenceOf(test.err)
			if got != test.want || window != test.wantWindow {
				t.Fatalf("evidence = (%t, %d), want (%t, %d); err=%v", got, window, test.want, test.wantWindow, test.err)
			}
		})
	}
}
