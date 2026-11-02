package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

const googleQuotaExhaustedBody = `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Quota exhausted","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"7200s"}]}}`

const googleRateLimitBody = `{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"Rate limited","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"7200s"}]}}`

func googleExecutorError(t *testing.T, provider, body string, stream bool) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	exec := task6Executor(provider)
	auth := &cliproxyauth.Auth{ID: "google-quota-" + provider, Provider: exec.Identifier(), Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gemini-2.5-pro", Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatGemini, Stream: stream}
	if !stream {
		_, err := exec.Execute(context.Background(), auth, req, opts)
		return err
	}
	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
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

func TestGeminiAndVertexReportGoogleQuotaExhaustion(t *testing.T) {
	for _, provider := range []string{"gemini", "vertex"} {
		for _, stream := range []bool{false, true} {
			name := provider + "/execute"
			if stream {
				name = provider + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				err := googleExecutorError(t, provider, googleQuotaExhaustedBody, stream)
				if err == nil {
					t.Fatal("expected an upstream 429 error")
				}
				exhausted, _ := quotaEvidenceOf(err)
				if !exhausted {
					t.Fatalf("QUOTA_EXHAUSTED 429 lost its evidence: %T %v", err, err)
				}
				resetErr, ok := errors.AsType[cliproxyexecutor.QuotaResetError](err)
				if !ok || resetErr.QuotaResetAfter() == nil || *resetErr.QuotaResetAfter() != 2*time.Hour {
					t.Fatalf("QUOTA_EXHAUSTED 429 lost its reset: %T %v", err, err)
				}
				// The default-mode error is unchanged: same text, no retry hint.
				if err.Error() != googleQuotaExhaustedBody {
					t.Fatalf("error text changed: %q", err.Error())
				}
				if hint, okHint := err.(interface{ RetryAfter() *time.Duration }); okHint && hint.RetryAfter() != nil {
					t.Fatalf("RetryAfter = %v, want nil as before", *hint.RetryAfter())
				}

				rateLimited := googleExecutorError(t, provider, googleRateLimitBody, stream)
				if exhausted, _ := quotaEvidenceOf(rateLimited); exhausted {
					t.Fatalf("RATE_LIMIT_EXCEEDED counted as quota exhaustion: %v", rateLimited)
				}
			})
		}
	}
}
