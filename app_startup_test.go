package daggo

import (
	"strings"
	"testing"
)

func TestConsoleBaseURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "default port style", addr: ":8000", want: "http://localhost:8000"},
		{name: "bind all ipv4", addr: "0.0.0.0:8000", want: "http://localhost:8000"},
		{name: "bind all ipv6", addr: "[::]:8000", want: "http://localhost:8000"},
		{name: "explicit host", addr: "127.0.0.1:9000", want: "http://127.0.0.1:9000"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := consoleBaseURL(tt.addr); got != tt.want {
				t.Fatalf("consoleBaseURL(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestStartupBannerIncludesConnectionHints(t *testing.T) {
	t.Parallel()

	banner := startupBanner(":8000")
	for _, snippet := range []string{
		"[DAGGO]",
		"UI:       http://localhost:8000/",
		"RPC docs: http://localhost:8000/rpc/docs",
		"RPC base: http://localhost:8000/rpc",
	} {
		if !strings.Contains(banner, snippet) {
			t.Fatalf("startup banner missing %q in %q", snippet, banner)
		}
	}
}
