package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
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

type usageLimitNoopExecutor struct{ id string }

func (e usageLimitNoopExecutor) Identifier() string { return e.id }
func (usageLimitNoopExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (usageLimitNoopExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (usageLimitNoopExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }
func (usageLimitNoopExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (usageLimitNoopExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

// Only credentials selection could run count: one whose provider has no
// executor cannot serve the request and must not block the usage limit.
func TestUsageLimitPoolIgnoresCredentialsWithoutExecutor(t *testing.T) {
	const model = "usage-limit-mixed-model"
	now := time.Now()
	for _, tc := range []struct {
		name          string
		otherExecutor bool
		want          bool
	}{
		{name: "other provider has no executor", otherExecutor: false, want: true},
		{name: "other provider can serve", otherExecutor: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			m.RegisterExecutor(usageLimitNoopExecutor{id: "quota-a"})
			if tc.otherExecutor {
				m.RegisterExecutor(usageLimitNoopExecutor{id: "quota-b"})
			}
			exhausted := credentialQuotaAuth("mixed-a-"+tc.name, true, now.Add(2*time.Hour), 300)
			exhausted.Provider = "quota-a"
			available := &Auth{ID: "mixed-b-" + tc.name, Provider: "quota-b", Status: StatusActive}
			for _, auth := range []*Auth{exhausted, available} {
				if _, err := m.Register(WithSkipPersist(context.Background()), auth); err != nil {
					t.Fatal(err)
				}
				registryRegister(t, auth.ID, auth.Provider, model)
			}
			refusal := m.usageLimitRefusalForPool([]string{"quota-a", "Quota-B"}, model, authSelectionEligibility{}, "", now)
			if (refusal != nil) != tc.want {
				t.Fatalf("refusal = %+v, want usage limit %t", refusal, tc.want)
			}
		})
	}
}

func registryRegister(t *testing.T, authID, provider, model string) {
	t.Helper()
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(authID) })
}
