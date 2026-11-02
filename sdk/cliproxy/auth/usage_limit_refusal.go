package auth

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// UsageLimitMinResetIn separates cooldowns a client should wait out through
// Retry-After from exhausted quota it should stop on. A pool whose earliest
// reset is this close or closer keeps the retryable model_cooldown response.
const UsageLimitMinResetIn = 5 * time.Minute

// codexWeeklyWindowMinutes is the shortest window Codex reports in its
// secondary rate-limit slot; shorter windows use the primary slot.
const codexWeeklyWindowMinutes = 24 * 60

// usageLimitRefusal describes a pool whose every eligible credential is out of
// quota: when the first one reopens and, when the upstream named it, the
// length of that credential's exhausted window.
type usageLimitRefusal struct {
	resetAt       time.Time
	windowMinutes int64
}

func (e *modelCooldownError) withUsageLimit(refusal *usageLimitRefusal) *modelCooldownError {
	if e != nil {
		e.usageLimit = refusal
	}
	return e
}

// NewUsageLimitModelCooldownError creates a model cooldown reported as a final
// usage limit, as the usage-limit response mode does when every candidate
// credential is out of quota until resetAt. windowMinutes may be 0 when the
// exhausted window is unknown.
func NewUsageLimitModelCooldownError(model, provider string, resetAt time.Time, windowMinutes int64) error {
	return newModelCooldownError(model, provider, time.Until(resetAt)).withUsageLimit(&usageLimitRefusal{
		resetAt:       resetAt,
		windowMinutes: windowMinutes,
	})
}

// UsageLimitResetAt reports when the first credential reopens if err carries a
// final usage-limit cooldown.
func UsageLimitResetAt(err error) (time.Time, bool) {
	var cooldown *modelCooldownError
	if !errors.As(err, &cooldown) || cooldown == nil || cooldown.usageLimit == nil {
		return time.Time{}, false
	}
	return cooldown.usageLimit.resetAt, true
}

// IsUsageLimitReachedError reports whether err carries a final usage-limit
// cooldown.
func IsUsageLimitReachedError(err error) bool {
	_, ok := UsageLimitResetAt(err)
	return ok
}

// UsageLimitResponseHeaders returns the client-safe headers of the final
// usage-limit cooldown in err's chain: X-Should-Retry: false and, when the
// exhausted window is known, Codex's rate-limit headers. It never includes
// Retry-After.
func UsageLimitResponseHeaders(err error) (http.Header, bool) {
	var cooldown *modelCooldownError
	if !errors.As(err, &cooldown) || cooldown == nil || cooldown.usageLimit == nil {
		return nil, false
	}
	return cooldown.Headers(), true
}

// codexRateLimitHeaders reports the exhausted window in the rate-limit headers
// Codex reads with a usage_limit_reached response. Codex puts its 5-hour
// window in the primary slot and its weekly window in the secondary slot. An
// unknown window gets no headers: the slot would be a guess.
func (r *usageLimitRefusal) codexRateLimitHeaders(headers http.Header) {
	if r == nil || headers == nil || r.windowMinutes <= 0 {
		return
	}
	slot := "Primary"
	if r.windowMinutes >= codexWeeklyWindowMinutes {
		slot = "Secondary"
	}
	prefix := "X-Codex-" + slot + "-"
	headers.Set(prefix+"Used-Percent", "100")
	headers.Set(prefix+"Window-Minutes", strconv.FormatInt(r.windowMinutes, 10))
	headers.Set(prefix+"Reset-At", strconv.FormatInt(r.resetAt.Unix(), 10))
}

// usageLimitRefusalFor reports the usage limit that blocks every candidate, or
// nil when there is no candidate, any candidate is unblocked or blocked for
// another reason, or the first reset is UsageLimitMinResetIn away or closer.
// Only explicit quota-exhaustion evidence tied to the blocking deadline counts:
// a long or repeated generic 429 stays retryable.
func usageLimitRefusalFor(auths []*Auth, modelFor func(*Auth) string, now time.Time) *usageLimitRefusal {
	var refusal *usageLimitRefusal
	for _, auth := range auths {
		model := ""
		if modelFor != nil {
			model = modelFor(auth)
		}
		blocked, reason, next := isAuthBlockedForModel(auth, model, now)
		if !blocked || reason != blockReasonCooldown || !next.After(now) {
			return nil
		}
		window, ok := quotaExhaustionEvidence(auth, model, now)
		if !ok {
			return nil
		}
		if refusal == nil || next.Before(refusal.resetAt) || (next.Equal(refusal.resetAt) && refusal.windowMinutes == 0) {
			refusal = &usageLimitRefusal{resetAt: next, windowMinutes: window}
		}
	}
	if refusal == nil || refusal.resetAt.Sub(now) <= UsageLimitMinResetIn {
		return nil
	}
	return refusal
}

// quotaExhaustionEvidence reports whether the cooldown that blocks auth for
// model is backed by live, explicit upstream quota-exhaustion evidence whose
// recovery time covers the blocking deadline, and the exhausted window length
// when known. It mirrors isAuthBlockedForModel: a live credential-wide quota
// decides alone; otherwise every blocking state of the model must carry the
// evidence.
func quotaExhaustionEvidence(auth *Auth, model string, now time.Time) (int64, bool) {
	if auth == nil {
		return 0, false
	}
	if auth.Quota.Exceeded && auth.Quota.Reason == "credential_quota" && auth.Quota.NextRecoverAt.After(now) {
		if !auth.Quota.Exhausted {
			return 0, false
		}
		return auth.Quota.LimitWindowMinutes, true
	}
	if model == "" || len(auth.ModelStates) == 0 {
		_, _, deadline := availabilityBlock(auth.Unavailable, auth.Quota.Exceeded, auth.NextRetryAfter, auth.Quota.NextRecoverAt, now)
		if !quotaExhaustionCovers(auth.Quota, deadline, now) {
			return 0, false
		}
		return auth.Quota.LimitWindowMinutes, true
	}
	modelKey := canonicalModelKey(model)
	evidenced := false
	var window int64
	var latest time.Time
	for stateModel, state := range auth.ModelStates {
		if state == nil || canonicalModelKey(stateModel) != modelKey {
			continue
		}
		blocked, _, deadline := availabilityBlock(state.Unavailable, state.Quota.Exceeded, state.NextRetryAfter, state.Quota.NextRecoverAt, now)
		if !blocked {
			continue
		}
		if !quotaExhaustionCovers(state.Quota, deadline, now) {
			return 0, false
		}
		if !evidenced || deadline.After(latest) {
			window = state.Quota.LimitWindowMinutes
			latest = deadline
		}
		evidenced = true
	}
	return window, evidenced
}

// quotaExhaustionCovers reports whether quota holds live exhaustion evidence
// that lasts at least until deadline.
func quotaExhaustionCovers(quota QuotaState, deadline, now time.Time) bool {
	return quota.Exceeded && quota.Exhausted && quota.NextRecoverAt.After(now) && !quota.NextRecoverAt.Before(deadline)
}

// usageLimitRefusalForPool evaluates every credential eligible for the request
// (the same candidates the retry scheduler considers, ignoring which ones this
// request already attempted) and reports the usage limit that blocks them all.
func (m *Manager) usageLimitRefusalForPool(providers []string, model string, eligibility authSelectionEligibility, pinnedAuthID string, now time.Time) *usageLimitRefusal {
	if m == nil || len(providers) == 0 {
		return nil
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		if key := strings.TrimSpace(strings.ToLower(provider)); key != "" {
			providerSet[key] = struct{}{}
		}
	}
	registryRef := registry.GetGlobalRegistry()
	m.mu.RLock()
	defer m.mu.RUnlock()
	candidates := make([]*Auth, 0, len(m.auths))
	for _, auth := range m.auths {
		if auth == nil || auth.Disabled || auth.Status == StatusDisabled {
			continue
		}
		if pinnedAuthID != "" && auth.ID != pinnedAuthID {
			continue
		}
		if !eligibility.allows(auth) {
			continue
		}
		if _, ok := providerSet[executorKeyFromAuth(auth)]; !ok {
			continue
		}
		if model != "" && !m.authSupportsRouteModel(registryRef, auth, model) {
			continue
		}
		candidates = append(candidates, auth)
	}
	return usageLimitRefusalFor(candidates, func(auth *Auth) string {
		if strings.TrimSpace(model) == "" {
			return model
		}
		return m.selectionModelForAuth(auth, model)
	}, now)
}

// usageLimitResponseError applies the usage-limit response mode to the final
// error of a request, before any response bytes are committed. When the mode
// is active, the final error is a 429, and every eligible credential is out of
// quota with a reset more than UsageLimitMinResetIn away, it returns a model
// cooldown reported as a final usage limit. Otherwise it returns nil and the
// caller keeps its error, so the retryable mode keeps today's behavior.
func (m *Manager) usageLimitResponseError(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, err error) *modelCooldownError {
	if err == nil || m == nil || !QuotaCooldownUsageLimitModeEnabled() || m.HomeEnabled() {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	if statusCodeFromError(err) != http.StatusTooManyRequests {
		return nil
	}
	now := time.Now()
	refusal := m.usageLimitRefusalForPool(providers, model, authSelectionEligibilityForRequest(ctx, opts), pinnedAuthIDFromMetadata(opts.Metadata), now)
	if refusal == nil {
		return nil
	}
	modelName := model
	provider := ""
	if len(providers) == 1 {
		provider = providers[0]
	}
	cause := err
	var cooldown *modelCooldownError
	if errors.As(err, &cooldown) && cooldown != nil {
		// All credentials were already cooling at selection: keep the
		// selection error's naming and last-error cause.
		if cooldown.model != "" {
			modelName = cooldown.model
		}
		provider = cooldown.provider
		cause = cooldown.cause
	}
	return newModelCooldownErrorWithCause(modelName, provider, refusal.resetAt.Sub(now), cause).withUsageLimit(refusal)
}
