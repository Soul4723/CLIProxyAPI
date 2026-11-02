package auth

import (
	"bytes"
	"context"
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// withStreamUsageLimit applies the usage-limit response mode to a stream whose
// response may already be committed. When a 429 ends the stream and every
// eligible credential is then out of quota with a reset more than
// UsageLimitMinResetIn away, the stream's final error becomes the usage-limit
// cooldown, so the handler's terminal error event carries the reset. The
// failure itself is still recorded once, by wrapStreamResult, before its error
// chunk reaches this wrapper.
//
// An upstream quota failure frame that the executor forwards right before its
// error chunk (Codex duplex streams do this) is held back until that chunk
// arrives. If the usage limit replaces the error, the quota event is removed
// from that chunk so the client gets one terminal error, not two, and any other
// events in the chunk are still sent; otherwise the chunk is sent unchanged.
//
// In the retryable mode, and under Home, the stream is returned as is.
func (m *Manager) withStreamUsageLimit(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, result *cliproxyexecutor.StreamResult) *cliproxyexecutor.StreamResult {
	if m == nil || result == nil || result.Chunks == nil || !QuotaCooldownUsageLimitModeEnabled() || m.HomeEnabled() {
		return result
	}
	in := result.Chunks
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		send := func(chunk cliproxyexecutor.StreamChunk) bool {
			if ctx == nil {
				out <- chunk
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case out <- chunk:
				return true
			}
		}
		var held *cliproxyexecutor.StreamChunk
		for chunk := range in {
			if chunk.Err != nil {
				if usageLimitErr := m.usageLimitResponseError(ctx, providers, model, opts, chunk.Err); usageLimitErr != nil {
					chunk.Err = usageLimitErr
					if held != nil {
						// Drop only the quota event; other events coalesced
						// into the same chunk are still sent.
						if rest := withoutQuotaFailureEvents(held.Payload); len(rest) > 0 {
							held.Payload = rest
						} else {
							held = nil
						}
					}
				}
			}
			if held != nil {
				if !send(*held) {
					discardStreamChunks(in)
					return
				}
				held = nil
			}
			if chunk.Err == nil && streamPayloadIsQuotaFailure(chunk.Payload) {
				pending := chunk
				held = &pending
				continue
			}
			if !send(chunk) {
				discardStreamChunks(in)
				return
			}
		}
		if held != nil {
			send(*held)
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}
}

// streamPayloadIsQuotaFailure reports whether a stream payload is an upstream
// error event for a rate limit or exhausted quota, in raw JSON or SSE framing.
func streamPayloadIsQuotaFailure(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return false
	}
	if trimmed[0] == '{' {
		return ojson.ValidBytes(trimmed) && jsonIsQuotaFailure(trimmed)
	}
	for _, line := range bytes.Split(trimmed, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(line[len("data:"):])
		if len(data) > 0 && data[0] == '{' && ojson.ValidBytes(data) && jsonIsQuotaFailure(data) {
			return true
		}
	}
	return false
}

func jsonIsQuotaFailure(data []byte) bool {
	root := ojson.ParseBytes(data)
	eventType := root.Get("type").String()
	errorNode, ok := streamFailureErrorNode(root, eventType)
	if !ok || root.Get("choices").Exists() {
		return false
	}
	if eventType != "" && eventType != "error" && eventType != "response.failed" {
		return false
	}
	for _, status := range []ojson.Result{root.Get("status"), root.Get("status_code"), errorNode.Get("status"), errorNode.Get("status_code")} {
		if status.Int() == http.StatusTooManyRequests {
			return true
		}
	}
	for _, value := range []string{errorNode.Get("type").String(), errorNode.Get("code").String(), errorNode.Get("status").String()} {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "usage_limit_reached", "insufficient_quota", "rate_limit_exceeded", "rate_limit_error", "resource_exhausted":
			return true
		}
	}
	return false
}

// streamFailureErrorNode finds the error object of a failure event in the
// places the Codex executors read it, in their order: body.error and error for
// a WebSocket error event, response.error and error for response.failed.
func streamFailureErrorNode(root ojson.Result, eventType string) (ojson.Result, bool) {
	paths := []string{"body.error", "error"}
	if eventType == "response.failed" {
		paths = []string{"response.error", "error"}
	}
	for _, path := range paths {
		if node := root.Get(path); node.IsObject() {
			return node, true
		}
	}
	return ojson.Result{}, false
}

// withoutQuotaFailureEvents returns payload without its quota failure events. A
// raw JSON payload is one event; an SSE payload is split at blank lines and the
// other events keep their exact bytes.
func withoutQuotaFailureEvents(payload []byte) []byte {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		if streamPayloadIsQuotaFailure(trimmed) {
			return nil
		}
		return payload
	}
	var out, event []byte
	flush := func() {
		if len(event) > 0 && !streamPayloadIsQuotaFailure(event) {
			out = append(out, event...)
		}
		event = event[:0:0]
	}
	for _, line := range bytes.SplitAfter(payload, []byte("\n")) {
		event = append(event, line...)
		if len(bytes.TrimSpace(line)) == 0 {
			flush()
		}
	}
	flush()
	if len(bytes.TrimSpace(out)) == 0 {
		return nil
	}
	return out
}
