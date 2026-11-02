package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestQuotaCooldownResponseModeYAMLRoundTrip(t *testing.T) {
	const v8Path = "routing.cooldown.quota-response-mode"
	const legacyPath = "quota-cooldown-response-mode"
	for _, tc := range []struct {
		name      string
		raw       string
		want      string
		savedPath string
	}{
		{name: "v8 key", raw: "routing:\n  cooldown:\n    quota-response-mode: usage-limit\n", want: QuotaCooldownResponseModeUsageLimit, savedPath: v8Path},
		{name: "v8 key normalized", raw: "routing:\n  cooldown:\n    quota-response-mode: ' Usage-Limit '\n", want: QuotaCooldownResponseModeUsageLimit, savedPath: v8Path},
		// A legacy file keeps its legacy layout on save.
		{name: "legacy key", raw: "quota-cooldown-response-mode: usage-limit\n", want: QuotaCooldownResponseModeUsageLimit, savedPath: legacyPath},
		{name: "legacy retryable", raw: "quota-cooldown-response-mode: retryable\n", want: QuotaCooldownResponseModeRetryable, savedPath: legacyPath},
		{name: "absent means retryable", raw: "port: 8317\n", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseConfigBytes([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseConfigBytes: %v", err)
			}
			if parsed.QuotaCooldownResponseMode != tc.want {
				t.Fatalf("parsed mode = %q, want %q", parsed.QuotaCooldownResponseMode, tc.want)
			}

			path := filepath.Join(t.TempDir(), "config.yaml")
			if err = os.WriteFile(path, []byte(tc.raw), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.QuotaCooldownResponseMode != tc.want {
				t.Fatalf("loaded mode = %q, want %q", cfg.QuotaCooldownResponseMode, tc.want)
			}
			if err = SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatalf("save: %v", err)
			}
			reloaded, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("reload: %v", err)
			}
			if reloaded.QuotaCooldownResponseMode != tc.want {
				t.Fatalf("mode after save = %q, want %q", reloaded.QuotaCooldownResponseMode, tc.want)
			}
			saved, _ := os.ReadFile(path)
			var doc yaml.Node
			if err = yaml.Unmarshal(saved, &doc); err != nil {
				t.Fatal(err)
			}
			if tc.want == "" {
				if strings.Contains(string(saved), "quota-response-mode") || strings.Contains(string(saved), "quota-cooldown-response-mode") {
					t.Fatalf("save added an unset mode:\n%s", saved)
				}
				return
			}
			if node := yamlPath(doc.Content[0], tc.savedPath); node == nil || node.Value != tc.want {
				t.Fatalf("saved config lacks %s=%s:\n%s", tc.savedPath, tc.want, saved)
			}
		})
	}
}

func TestQuotaCooldownResponseModeRejectsUnknownValue(t *testing.T) {
	for _, raw := range []string{
		"routing:\n  cooldown:\n    quota-response-mode: sometimes\n",
		"quota-cooldown-response-mode: final\n",
	} {
		if _, err := ParseConfigBytes([]byte(raw)); err == nil || !strings.Contains(err.Error(), "quota-response-mode") {
			t.Fatalf("ParseConfigBytes(%q) error = %v, want an invalid quota-response-mode error", raw, err)
		}
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("LoadConfig(%q) accepted an unknown mode", raw)
		}
	}
}
