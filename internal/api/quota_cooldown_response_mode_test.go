package api

import (
	"os"
	"path/filepath"
	"testing"

	proxyconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestServerAppliesQuotaCooldownResponseModeAtStartupAndReload(t *testing.T) {
	t.Cleanup(func() { auth.SetQuotaCooldownResponseMode("") })
	auth.SetQuotaCooldownResponseMode("")

	cfg := &proxyconfig.Config{
		SDKConfig:                 sdkconfig.SDKConfig{APIKeys: []string{"test-key"}},
		QuotaCooldownResponseMode: proxyconfig.QuotaCooldownResponseModeUsageLimit,
	}
	server := newTestServerWithConfig(t, cfg)
	if !auth.QuotaCooldownUsageLimitModeEnabled() {
		t.Fatal("startup did not apply quota-response-mode usage-limit")
	}

	reloaded := *cfg
	reloaded.QuotaCooldownResponseMode = proxyconfig.QuotaCooldownResponseModeRetryable
	server.UpdateClients(&reloaded)
	if auth.QuotaCooldownUsageLimitModeEnabled() {
		t.Fatal("reload to retryable kept the usage-limit mode")
	}

	again := reloaded
	again.QuotaCooldownResponseMode = proxyconfig.QuotaCooldownResponseModeUsageLimit
	server.UpdateClients(&again)
	if !auth.QuotaCooldownUsageLimitModeEnabled() {
		t.Fatal("reload to usage-limit did not apply")
	}
}

func TestQuotaCooldownResponseModeHotReloadFromConfigFile(t *testing.T) {
	t.Cleanup(func() { auth.SetQuotaCooldownResponseMode("") })
	auth.SetQuotaCooldownResponseMode("")
	server := newTestServer(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, step := range []struct {
		yaml string
		want bool
	}{
		{yaml: "routing:\n  cooldown:\n    quota-response-mode: usage-limit\n", want: true},
		{yaml: "quota-cooldown-response-mode: retryable\n", want: false},
		{yaml: "quota-cooldown-response-mode: usage-limit\n", want: true},
		{yaml: "port: 8317\n", want: false},
	} {
		if err := os.WriteFile(path, []byte(step.yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := proxyconfig.LoadConfig(path)
		if err != nil {
			t.Fatalf("load %q: %v", step.yaml, err)
		}
		cfg.AuthDir = t.TempDir()
		server.UpdateClients(cfg)
		if got := auth.QuotaCooldownUsageLimitModeEnabled(); got != step.want {
			t.Fatalf("after reloading %q usage-limit mode = %t, want %t", step.yaml, got, step.want)
		}
	}
}
