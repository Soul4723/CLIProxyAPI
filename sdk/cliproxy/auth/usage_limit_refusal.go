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

// usageLimitCandidate is one credential eligible for the request: the model
// selection checks it under, and the state models normal execution records its
// failures under (one per pooled backend).
type usageLimitCandidate struct {
	auth           *Auth
	selectionModel string
	stateModels    []string
}

// usageLimitRefusalFor reports the usage limit that blocks every candidate, or
// nil when there is no candidate, any candidate can still serve the request or
// is blocked for another reason, or the first reset is UsageLimitMinResetIn
// away or closer. Only explicit quota-exhaustion evidence tied to the blocking
// deadline counts: a long or repeated generic 429 stays retryable.
func usageLimitRefusalFor(auths []*Auth, modelFor func(*Auth) string, now time.Time) *usageLimitRefusal {
	candidates := make([]usageLimitCandidate, 0, len(auths))
	for _, auth := range auths {
		model := ""
		if modelFor != nil {
			model = modelFor(auth)
		}
		candidates = append(candidates, usageLimitCandidate{auth: auth, selectionModel: model})
	}
	return usageLimitRefusalForCandidates(candidates, now)
}

func usageLimitRefusalForCandidates(candidates []usageLimitCandidate, now time.Time) *usageLimitRefusal {
	var refusal *usageLimitRefusal
	for _, candidate := range candidates {
		resetAt, window, ok := candidateQuotaBlock(candidate, now)
		if !ok {
			return nil
		}
		if refusal == nil || resetAt.Before(refusal.resetAt) || (resetAt.Equal(refusal.resetAt) && refusal.windowMinutes == 0) {
			refusal = &usageLimitRefusal{resetAt: resetAt, windowMinutes: window}
		}
	}
	if refusal == nil || refusal.resetAt.Sub(now) <= UsageLimitMinResetIn {
		return nil
	}
	return refusal
}

// candidateQuotaBlock reports when a credential can serve the request again,
// and the exhausted window length when known, if what blocks it is explicit
// quota exhaustion. Selection blocks a credential under its selection model;
// otherwise it can still serve the request through any execution model whose
// state is not blocked, so every one of those must be blocked by evidence and
// the credential reopens with the first of them.
func candidateQuotaBlock(candidate usageLimitCandidate, now time.Time) (time.Time, int64, bool) {
	auth := candidate.auth
	if blocked, reason, next := isAuthBlockedForModel(auth, candidate.selectionModel, now); blocked {
		if reason != blockReasonCooldown || !next.After(now) {
			return time.Time{}, 0, false
		}
		window, ok := quotaExhaustionEvidence(auth, candidate.selectionModel, now)
		return next, window, ok
	}
	if len(candidate.stateModels) == 0 {
		return time.Time{}, 0, false
	}
	var resetAt time.Time
	var window int64
	for _, stateModel := range candidate.stateModels {
		blocked, reason, next := isAuthBlockedForModel(auth, stateModel, now)
		if !blocked || reason != blockReasonCooldown || !next.After(now) {
			return time.Time{}, 0, false
		}
		stateWindow, ok := quotaExhaustionEvidence(auth, stateModel, now)
		if !ok {
			return time.Time{}, 0, false
		}
		if resetAt.IsZero() || next.Before(resetAt) {
			resetAt, window = next, stateWindow
		}
	}
	return resetAt, window, true
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
	evaluated := make([]usageLimitCandidate, 0, len(candidates))
	for _, auth := range candidates {
		candidate := usageLimitCandidate{auth: auth, selectionModel: model}
		if strings.TrimSpace(model) != "" {
			candidate.selectionModel = m.selectionModelForAuth(auth, model)
			upstreamModels, pooled := m.executionModelCandidatesForInspection(auth, model)
			for _, upstreamModel := range upstreamModels {
				candidate.stateModels = append(candidate.stateModels, m.stateModelForExecution(auth, model, upstreamModel, pooled))
			}
		}
		evaluated = append(evaluated, candidate)
	}
	return usageLimitRefusalForCandidates(evaluated, now)
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
	if statusCodeFromError(err) != http.StatusTooManyRequests && !isNoExecutableCredentialError(err) {
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

// isNoExecutableCredentialError reports the error a request ends with when no
// credential could run it at all, for example when selection still offers a
// pooled alias whose every backend is cooling. The pool check alone decides
// whether that is a usage limit.
func isNoExecutableCredentialError(err error) bool {
	var authErr *Error
	return errors.As(err, &authErr) && authErr != nil && authErr.Code == "auth_not_found"
}
