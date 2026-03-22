package daggo

import (
	_ "embed"
	"strings"
)

var (
	//go:embed VERSION
	versionFile string
)

func Version() string {
	trimmed := strings.TrimSpace(versionFile)
	if trimmed == "" {
		return "v0.0.0"
	}
	if strings.HasPrefix(trimmed, "v") {
		return trimmed
	}
	return "v" + trimmed
}
