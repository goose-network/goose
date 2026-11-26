package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLogConfigResolveDefaults(t *testing.T) {
	path, size, total := LogConfig{File: "logs/goose.log"}.Resolve()
	if path != "logs/goose.log" {
		t.Fatalf("path = %q", path)
	}
	if size != 32_000_000 || total != 1_000_000_000 {
		t.Fatalf("defaults = (%d, %d), want (32e6, 1e9)", size, total)
	}
}

func TestLogConfigResolveHardCap(t *testing.T) {
	// A budget above the cap is clamped to exactly 1 GB (10^9 bytes).
	_, size, total := LogConfig{File: "x", MaxSizeMB: 5000, MaxTotalMB: 99999}.Resolve()
	if total != 1_000_000_000 {
		t.Fatalf("total = %d, want 1e9", total)
	}
	if size != 1_000_000_000 {
		t.Fatalf("size = %d, want clamped to total", size)
	}
}

func TestLogConfigResolveClampsSizeToTotal(t *testing.T) {
	_, size, total := LogConfig{File: "x", MaxSizeMB: 500, MaxTotalMB: 100}.Resolve()
	if total != 100_000_000 || size != 100_000_000 {
		t.Fatalf("got (%d, %d), want size clamped to total (100e6, 100e6)", size, total)
	}
}

func TestLogConfigResolveDisable(t *testing.T) {
	path, _, _ := LogConfig{File: "-"}.Resolve()
	if path != "" {
		t.Fatalf("path = %q, want empty for \"-\"", path)
	}
	path, _, _ = LogConfig{}.Resolve()
	if path != "" {
		t.Fatalf("path = %q, want empty for unset file", path)
	}
}

func TestDefaultLogPath(t *testing.T) {
	if got := DefaultLogPath("/data/goose.json"); got != "/data/logs/goose.log" {
		t.Fatalf("DefaultLogPath absolute = %q", got)
	}
	got := DefaultLogPath("goose.json")
	want := filepath.Join(mustAbs(t, "."), "logs", "goose.log")
	if got != want {
		t.Fatalf("DefaultLogPath relative = %q, want %q", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("DefaultLogPath must be absolute, got %q", got)
	}
}

func mustAbs(t *testing.T, p string) string {
	t.Helper()
	abs, err := filepath.Abs(p)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func TestSamePath(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "x.db")
	if err := os.WriteFile(a, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		a, b string
		want bool
	}{
		{a, a, true},
		{a, filepath.Join(dir, ".", "x.db"), true},
		{"", a, false},
		{a, "", false},
		{a, filepath.Join(dir, "y.db"), false},
	}
	for _, c := range cases {
		if got := SamePath(c.a, c.b); got != c.want {
			t.Errorf("SamePath(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
	link := filepath.Join(dir, "link.db")
	if err := os.Symlink(a, link); err == nil {
		if !SamePath(link, a) {
			t.Error("symlink alias should compare equal")
		}
	}
	hard := filepath.Join(dir, "hard.db")
	if err := os.Link(a, hard); err == nil {
		if !SamePath(hard, a) {
			t.Error("hardlink alias should compare equal")
		}
	}
}