package helps

import (
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// GoogleBodyIndicatesQuotaExhausted reports whether body is a Google
// RESOURCE_EXHAUSTED error whose google.rpc.ErrorInfo reason is
// QUOTA_EXHAUSTED. Keyword matches and RATE_LIMIT_EXCEEDED, however long its
// retry delay, are not explicit quota exhaustion.
func GoogleBodyIndicatesQuotaExhausted(body []byte) bool {
	if !strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, "error.status").String()), "RESOURCE_EXHAUSTED") {
		return false
	}
	for _, detail := range gjson.GetBytes(body, "error.details").Array() {
		if detail.Get("@type").String() != "type.googleapis.com/google.rpc.ErrorInfo" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(detail.Get("reason").String()), "QUOTA_EXHAUSTED") {
			return true
		}
	}
	return false
}

// GoogleQuotaExhaustion reports whether a Google error response is an
// explicit quota exhaustion, and when the quota returns: the
// google.rpc.RetryInfo retryDelay, else a positive Retry-After header. The
// reset is nil when neither is present.
func GoogleQuotaExhaustion(statusCode int, headers http.Header, body []byte, now time.Time) (bool, *time.Duration) {
	if statusCode != http.StatusTooManyRequests || !GoogleBodyIndicatesQuotaExhausted(body) {
		return false, nil
	}
	if delay, err := ParseRetryDelay(body); err == nil && delay != nil && *delay > 0 {
		return true, delay
	}
	if raw := strings.TrimSpace(getHeaderCaseInsensitive(headers, "Retry-After")); raw != "" {
		if retryAt, ok := parseRetryAfterHeader(raw, now); ok && retryAt.After(now) {
			reset := retryAt.Sub(now)
			return true, &reset
		}
	}
	return true, nil
}
