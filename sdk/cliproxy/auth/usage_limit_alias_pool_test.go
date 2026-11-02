package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// poolQuotaErr is an explicit upstream quota exhaustion with a 2h reset.
type poolQuotaErr struct{}

func (poolQuotaErr) Error() string {
	return `{"error":{"type":"insufficient_quota","code":"insufficient_quota","message":"quota exhausted"}}`
}
func (poolQuotaErr) StatusCode() int            { return http.StatusTooManyRequests }
func (poolQuotaErr) RetryAfter() *time.Duration { d := 2 * time.Hour; return &d }
func (poolQuotaErr) IsQuotaExhausted() bool     { return true }

func usageLimitPoolRun(t *testing.T, m *Manager, model string, stream bool) error {
	t.Helper()
	req := cliproxyexecutor.Request{Model: model}
	if !stream {
		_, err := m.Execute(context.Background(), []string{openAICompatPoolProviderKey}, req, cliproxyexecutor.Options{})
		return err
	}
	result, err := m.ExecuteStream(context.Background(), []string{openAICompatPoolProviderKey}, req, cliproxyexecutor.Options{Stream: true})
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

// An alias pooled over several backends records cooldowns under the backend
// model names; the usage-limit check must read those states.
func TestUsageLimitModeCoversOpenAICompatAliasPool(t *testing.T) {
	SetQuotaCooldownResponseMode("usage-limit")
	t.Cleanup(func() { SetQuotaCooldownResponseMode("") })
	const alias = "public"
	for _, stream := range []bool{false, true} {
		name := "execute"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			executor := &openAICompatPoolExecutor{
				id:                openAICompatPoolProviderKey,
				executeErrors:     map[string]error{"backend-a": poolQuotaErr{}, "backend-b": poolQuotaErr{}},
				streamFirstErrors: map[string]error{"backend-a": poolQuotaErr{}, "backend-b": poolQuotaErr{}},
			}
			m := newOpenAICompatPoolTestManager(t, alias, []internalconfig.OpenAICompatibilityModel{
				{Name: "backend-a", Alias: alias},
				{Name: "backend-b", Alias: alias},
			}, executor)
			for _, path := range []string{"b_last_backend_refuses", "a_all_cooling"} {
				err := usageLimitPoolRun(t, m, alias, stream)
				resetAt, ok := UsageLimitResetAt(err)
				if !ok {
					t.Fatalf("%s: err = %v, want a usage-limit cooldown", path, err)
				}
				if wait := time.Until(resetAt); wait < 2*time.Hour-time.Minute || wait > 2*time.Hour+time.Minute {
					t.Fatalf("%s: reset in %v, want about 2h", path, wait)
				}
			}
		})
	}
}

// One pooled backend still available keeps the retryable answer.
func TestUsageLimitModeAliasPoolWithAvailableBackendStaysRetryable(t *testing.T) {
	SetQuotaCooldownResponseMode("usage-limit")
	t.Cleanup(func() { SetQuotaCooldownResponseMode("") })
	const alias = "public"
	executor := &openAICompatPoolExecutor{id: openAICompatPoolProviderKey, executeErrors: map[string]error{"backend-a": poolQuotaErr{}}}
	m := newOpenAICompatPoolTestManager(t, alias, []internalconfig.OpenAICompatibilityModel{
		{Name: "backend-a", Alias: alias},
		{Name: "backend-b", Alias: alias},
	}, executor)
	for i := 0; i < 3; i++ {
		if err := usageLimitPoolRun(t, m, alias, false); err != nil {
			if IsUsageLimitReachedError(err) {
				t.Fatalf("request %d: usage limit with an available backend: %v", i, err)
			}
		}
	}
}
