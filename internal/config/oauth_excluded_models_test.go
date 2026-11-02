package config

import (
	"reflect"
	"testing"
)

func TestNormalizeOAuthExcludedModelsMergesCanonicalProviders(t *testing.T) {
	want := map[string][]string{"claude": {"a", "shared", "b"}}
	for iteration := 0; iteration < 100; iteration++ {
		entries := make(map[string][]string)
		if iteration%2 == 0 {
			entries["Claude"] = []string{" A ", "shared", ""}
			entries["claude"] = []string{"b", " SHARED ", "b"}
		} else {
			entries["claude"] = []string{"b", " SHARED ", "b"}
			entries["Claude"] = []string{" A ", "shared", ""}
		}
		entries[" claude "] = []string{" ", ""}
		entries[" "] = []string{"ignored"}
		got := NormalizeOAuthExcludedModels(entries)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: NormalizeOAuthExcludedModels() = %#v, want %#v", iteration, got, want)
		}
		if again := NormalizeOAuthExcludedModels(got); !reflect.DeepEqual(again, want) {
			t.Fatalf("normalization is not idempotent: %#v", again)
		}
	}
}
