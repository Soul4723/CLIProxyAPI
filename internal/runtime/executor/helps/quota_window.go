package helps

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const (
	claudeFiveHourWindowMinutes = 5 * 60
	claudeSevenDayWindowMinutes = 7 * 24 * 60
)

// CodexExhaustedWindowMinutes reports the length, in minutes, of the Codex
// rate-limit window that a usage_limit_reached refusal exhausted. It reads the
// x-codex-primary-* and x-codex-secondary-* response headers and picks the
// exhausted window (used percent at or above 100) that resets last, because
// the credential stays blocked until then. A limit_window_minutes field in the
// error body is used when the headers name no exhausted window. It returns 0
// when the window is unknown; callers must not guess one.
func CodexExhaustedWindowMinutes(headers http.Header, body []byte) int64 {
	now := float64(time.Now().Unix())
	var best int64
	var bestReset float64
	for _, slot := range []string{"primary", "secondary"} {
		prefix := "X-Codex-" + slot + "-"
		used, okUsed := parseHeaderFloat(headers, prefix+"used-percent")
		minutes, okMinutes := parseHeaderFloat(headers, prefix+"window-minutes")
		if !okUsed || used < 100 || !okMinutes || minutes <= 0 {
			continue
		}
		// Compare resets as unix seconds.
		reset, okReset := parseHeaderFloat(headers, prefix+"reset-at")
		if !okReset {
			if after, okAfter := parseHeaderFloat(headers, prefix+"reset-after-seconds"); okAfter {
				reset = now + after
			} else {
				// Without a reset time the longer window is the later one.
				reset = now + minutes*60
			}
		}
		if best == 0 || reset > bestReset {
			best = int64(minutes)
			bestReset = reset
		}
	}
	if best > 0 {
		return best
	}
	for _, path := range []string{"error.limit_window_minutes", "limit_window_minutes"} {
		if window := gjson.GetBytes(body, path).Int(); window > 0 {
			return window
		}
	}
	return 0
}

func parseHeaderFloat(headers http.Header, name string) (float64, bool) {
	if headers == nil {
		return 0, false
	}
	raw := strings.TrimSpace(getHeaderCaseInsensitive(headers, name))
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}
