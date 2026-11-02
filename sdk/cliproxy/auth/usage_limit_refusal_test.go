package auth

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func credentialQuotaAuth(id string, exhausted bool, recoverAt time.Time, window int64) *Auth {
	return &Auth{
		ID:          id,
		Provider:    "codex",
		Status:      StatusError,
		Unavailable: true,
		Quota: QuotaState{
			Exceeded:           true,
			Reason:             "credential_quota",
			NextRecoverAt:      recoverAt,
			Exhausted:          exhausted,
			LimitWindowMinutes: window,
		},
		NextRetryAfter: recoverAt,
	}
}

func TestUsageLimitRefusalForThresholdAndEvidence(t *testing.T) {
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		auths     []*Auth
		wantReset time.Time
		window    int64
	}{
		{name: "exactly five minutes stays retryable", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(UsageLimitMinResetIn), 300)}},
		{name: "just over five minutes is a usage limit", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(UsageLimitMinResetIn+time.Second), 300)}, wantReset: now.Add(UsageLimitMinResetIn + time.Second), window: 300},
		{name: "no evidence stays retryable", auths: []*Auth{credentialQuotaAuth("a", false, now.Add(2*time.Hour), 0)}},
		{name: "mixed pool stays retryable", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(2*time.Hour), 300), credentialQuotaAuth("b", false, now.Add(3*time.Hour), 0)}},
		{name: "earliest reset wins with its window", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(3*24*time.Hour), 10080), credentialQuotaAuth("b", true, now.Add(2*time.Hour), 300)}, wantReset: now.Add(2 * time.Hour), window: 300},
		{name: "unknown window is kept unknown", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(2*time.Hour), 0)}, wantReset: now.Add(2 * time.Hour)},
		{name: "available candidate stays retryable", auths: []*Auth{credentialQuotaAuth("a", true, now.Add(2*time.Hour), 300), {ID: "b", Provider: "codex", Status: StatusActive}}},
		{name: "empty pool", auths: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusal := usageLimitRefusalFor(tc.auths, func(*Auth) string { return "gpt-5" }, now)
			if tc.wantReset.IsZero() {
				if refusal != nil {
					t.Fatalf("refusal = %+v, want nil", refusal)
				}
				return
			}
			if refusal == nil {
				t.Fatal("refusal = nil, want a usage limit")
			}
			if !refusal.resetAt.Equal(tc.wantReset) || refusal.windowMinutes != tc.window {
				t.Fatalf("refusal = %+v, want reset %v window %d", refusal, tc.wantReset, tc.window)
			}
		})
	}
}

func TestQuotaExhaustionEvidenceMustCoverModelDeadline(t *testing.T) {
	now := time.Now()
	recover := now.Add(2 * time.Hour)
	auth := &Auth{ID: "a", Provider: "claude", Status: StatusActive, ModelStates: map[string]*ModelState{
		"claude-opus": {
			Unavailable:    true,
			Status:         StatusError,
			NextRetryAfter: recover,
			Quota:          QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: recover, Exhausted: true, LimitWindowMinutes: 300},
		},
	}}
	if window, ok := quotaExhaustionEvidence(auth, "claude-opus", now); !ok || window != 300 {
		t.Fatalf("evidence = (%d, %t), want (300, true)", window, ok)
	}
	// A later deadline set by something else is not covered by the evidence.
	auth.ModelStates["claude-opus"].NextRetryAfter = recover.Add(time.Hour)
	if _, ok := quotaExhaustionEvidence(auth, "claude-opus", now); ok {
		t.Fatal("evidence covered a longer deadline it did not set")
	}
}

func TestMarkResultRecordsQuotaEvidenceOnlyWithProviderReset(t *testing.T) {
	reset := 2 * time.Hour
	for _, tc := range []struct {
		name            string
		result          Result
		wantExhausted   bool
		wantWindow      int64
		credentialScope bool
	}{
		{name: "explicit exhaustion with reset", result: Result{QuotaExhausted: true, QuotaLimitWindowMinutes: 300, RetryAfter: &reset}, wantExhausted: true, wantWindow: 300},
		{name: "credential scoped exhaustion with reset", result: Result{QuotaExhausted: true, QuotaLimitWindowMinutes: 10080, RetryAfter: &reset, CredentialScope: true}, wantExhausted: true, wantWindow: 10080, credentialScope: true},
		{name: "explicit exhaustion without reset", result: Result{QuotaExhausted: true, QuotaLimitWindowMinutes: 300}},
		{name: "generic long rate limit", result: Result{RetryAfter: &reset}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			auth := &Auth{ID: "auth-evidence", Provider: "codex", Status: StatusActive}
			if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
				t.Fatal(err)
			}
			result := tc.result
			result.AuthID = auth.ID
			result.Provider = "codex"
			result.Model = "gpt-5"
			result.Error = &Error{Message: "limited", HTTPStatus: http.StatusTooManyRequests}
			manager.MarkResult(context.Background(), result)

			got, _ := manager.GetByID(auth.ID)
			quota := got.ModelStates["gpt-5"].Quota
			if tc.credentialScope {
				quota = got.Quota
			}
			if quota.Exhausted != tc.wantExhausted || quota.LimitWindowMinutes != tc.wantWindow {
				t.Fatalf("quota = %+v, want exhausted=%t window=%d", quota, tc.wantExhausted, tc.wantWindow)
			}
			refusal := usageLimitRefusalFor([]*Auth{got}, func(*Auth) string { return "gpt-5" }, time.Now())
			if (refusal != nil) != tc.wantExhausted {
				t.Fatalf("refusal = %+v, want usage limit %t", refusal, tc.wantExhausted)
			}

			// Evidence never outlives the cooldown it belongs to.
			manager.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: "codex", Model: "gpt-5", Success: true})
			got, _ = manager.GetByID(auth.ID)
			quotas := []QuotaState{got.Quota}
			if state := got.ModelStates["gpt-5"]; state != nil {
				quotas = append(quotas, state.Quota)
			}
			for _, quota := range quotas {
				if quota.Exhausted && !quota.Exceeded {
					t.Fatalf("quota evidence outlived its cooldown: %+v", quota)
				}
			}
		})
	}
}

func TestQuotaEvidenceSurvivesCooldownStoreRestart(t *testing.T) {
	reset := 2 * time.Hour
	for _, credentialScope := range []bool{true, false} {
		name := "model_scope"
		if credentialScope {
			name = "credential_scope"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "auths")
			auth := func() *Auth { return &Auth{ID: "auth-restart", Provider: "codex", Status: StatusActive} }

			before := NewManager(nil, nil, nil)
			before.SetCooldownStateStore(NewFileCooldownStateStore(dir))
			if _, err := before.Register(WithSkipPersist(context.Background()), auth()); err != nil {
				t.Fatal(err)
			}
			before.MarkResult(context.Background(), Result{
				AuthID: "auth-restart", Provider: "codex", Model: "gpt-5",
				Error:          &Error{Message: "usage_limit_reached", HTTPStatus: http.StatusTooManyRequests},
				RetryAfter:     &reset,
				QuotaExhausted: true, QuotaLimitWindowMinutes: 300,
				CredentialScope: credentialScope,
			})
			before.PersistCooldownStates(context.Background())

			after := NewManager(nil, nil, nil)
			after.SetCooldownStateStore(NewFileCooldownStateStore(dir))
			if _, err := after.Register(WithSkipPersist(context.Background()), auth()); err != nil {
				t.Fatal(err)
			}
			if err := after.RestoreCooldownStates(context.Background()); err != nil {
				t.Fatal(err)
			}
			restored, _ := after.GetByID("auth-restart")
			refusal := usageLimitRefusalFor([]*Auth{restored}, func(*Auth) string { return "gpt-5" }, time.Now())
			if refusal == nil {
				t.Fatalf("restored cooldown lost its quota evidence: auth=%+v states=%+v", restored.Quota, restored.ModelStates)
			}
			if refusal.windowMinutes != 300 {
				t.Fatalf("restored window = %d, want 300", refusal.windowMinutes)
			}
			if wait := time.Until(refusal.resetAt); wait < reset-time.Minute || wait > reset+time.Minute {
				t.Fatalf("restored reset in %v, want about %v", wait, reset)
			}
		})
	}
}

func TestUsageLimitCooldownHeadersNameOnlyKnownCodexWindows(t *testing.T) {
	resetAt := time.Now().Add(3 * 24 * time.Hour).Truncate(time.Second)
	for _, tc := range []struct {
		window   int64
		wantSlot string
	}{
		{window: 300, wantSlot: "Primary"},
		{window: 10080, wantSlot: "Secondary"},
		{window: 0},
	} {
		headers := newModelCooldownError("gpt-5", "codex", time.Until(resetAt)).withUsageLimit(&usageLimitRefusal{resetAt: resetAt, windowMinutes: tc.window}).Headers()
		if headers.Get("X-Should-Retry") != "false" || headers.Get("Retry-After") != "" {
			t.Fatalf("window %d: headers = %v, want X-Should-Retry false and no Retry-After", tc.window, headers)
		}
		for _, slot := range []string{"Primary", "Secondary"} {
			got := headers.Get("X-Codex-" + slot + "-Used-Percent")
			if slot == tc.wantSlot && got != "100" {
				t.Fatalf("window %d: %s used percent = %q, want 100", tc.window, slot, got)
			}
			if slot != tc.wantSlot && got != "" {
				t.Fatalf("window %d: %s used percent = %q, want absent", tc.window, slot, got)
			}
		}
	}
	// The retryable cooldown is unchanged.
	headers := newModelCooldownError("gpt-5", "codex", 30*time.Second).Headers()
	if headers.Get("Retry-After") != "30" || headers.Get("X-Should-Retry") != "" {
		t.Fatalf("retryable cooldown headers = %v", headers)
	}
}

func TestSetQuotaCooldownResponseMode(t *testing.T) {
	t.Cleanup(func() { SetQuotaCooldownResponseMode("") })
	for _, tc := range []struct {
		mode string
		want bool
	}{
		{"usage-limit", true}, {" Usage-Limit ", true}, {"retryable", false}, {"", false}, {"bogus", false},
	} {
		SetQuotaCooldownResponseMode(tc.mode)
		if got := QuotaCooldownUsageLimitModeEnabled(); got != tc.want {
			t.Fatalf("mode %q enabled = %t, want %t", tc.mode, got, tc.want)
		}
	}
}

type quotaResetOnlyErr struct{}

func (quotaResetOnlyErr) Error() string                   { return "quota exhausted" }
func (quotaResetOnlyErr) StatusCode() int                 { return http.StatusTooManyRequests }
func (quotaResetOnlyErr) IsQuotaExhausted() bool          { return true }
func (quotaResetOnlyErr) QuotaResetAfter() *time.Duration { d := 2 * time.Hour; return &d }

// A quota reset reported apart from the retry hint (Gemini and Vertex) sets
// the cooldown only in the usage-limit mode; the default keeps its backoff.
func TestQuotaResetWithoutRetryHintAppliesOnlyInUsageLimitMode(t *testing.T) {
	t.Cleanup(func() { SetQuotaCooldownResponseMode("") })
	for _, mode := range []string{"retryable", "usage-limit"} {
		t.Run(mode, func(t *testing.T) {
			SetQuotaCooldownResponseMode(mode)
			manager := NewManager(nil, nil, nil)
			if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-reset", Provider: "gemini", Status: StatusActive}); err != nil {
				t.Fatal(err)
			}
			result := Result{AuthID: "auth-reset", Provider: "gemini", Model: "gemini-2.5-pro", Error: &Error{Message: "quota exhausted", HTTPStatus: http.StatusTooManyRequests}}
			result.attachQuotaEvidence(quotaResetOnlyErr{})
			if result.RetryAfter != nil {
				t.Fatalf("attachQuotaEvidence set RetryAfter = %v", *result.RetryAfter)
			}
			manager.MarkResult(context.Background(), result)
			got, _ := manager.GetByID("auth-reset")
			state := got.ModelStates["gemini-2.5-pro"]
			wait := time.Until(state.NextRetryAfter)
			if mode == "retryable" {
				if state.Quota.Exhausted || wait > time.Hour {
					t.Fatalf("default mode changed: exhausted=%t cooldown=%v", state.Quota.Exhausted, wait)
				}
				return
			}
			if !state.Quota.Exhausted || wait < 2*time.Hour-time.Minute || wait > 2*time.Hour+time.Minute {
				t.Fatalf("usage-limit mode: exhausted=%t cooldown=%v, want evidence and about 2h", state.Quota.Exhausted, wait)
			}
		})
	}
}
