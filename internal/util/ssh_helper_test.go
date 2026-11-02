package util

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type publicIPTransport func(*http.Request) (*http.Response, error)

func (f publicIPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestGetPublicIPSkipsInvalidResponses(t *testing.T) {
	originalClient := http.DefaultClient
	t.Cleanup(func() { http.DefaultClient = originalClient })

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "IPv4", body: " 203.0.113.42\n", want: "203.0.113.42"},
		{name: "IPv6", body: " 2001:db8::42\n", want: "2001:db8::42"},
		{name: "all invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			http.DefaultClient = &http.Client{Transport: publicIPTransport(func(req *http.Request) (*http.Response, error) {
				body := "<html>service unavailable</html>"
				if calls == 1 && tc.body != "" {
					body = tc.body
				}
				calls++
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			})}

			got, err := getPublicIP()
			if tc.want == "" {
				if err == nil || got != "" || calls != len(ipServices) {
					t.Fatalf("getPublicIP() = %q, %v after %d calls; want error after all services", got, err, calls)
				}
				return
			}
			if err != nil || got != tc.want || calls != 2 {
				t.Fatalf("getPublicIP() = %q, %v after %d calls; want %q after 2 calls", got, err, calls, tc.want)
			}
		})
	}
}
