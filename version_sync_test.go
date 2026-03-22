package daggo

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestVersionMatchesRootVersionFile(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("VERSION")
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}
	if got, want := Version(), strings.TrimSpace(string(data)); got != want {
		t.Fatalf("Version() = %q, want %q", got, want)
	}
}

func TestVersionSyncsWithFrontendPackageAndChangelog(t *testing.T) {
	t.Parallel()

	version := Version()
	if !strings.HasPrefix(version, "v") {
		t.Fatalf("Version() should include a leading v, got %q", version)
	}

	var pkg struct {
		Version string `json:"version"`
	}
	packageJSON, err := os.ReadFile("frontend-web/package.json")
	if err != nil {
		t.Fatalf("read frontend package.json: %v", err)
	}
	if err := json.Unmarshal(packageJSON, &pkg); err != nil {
		t.Fatalf("unmarshal frontend package.json: %v", err)
	}
	if got, want := strings.TrimSpace(pkg.Version), strings.TrimPrefix(version, "v"); got != want {
		t.Fatalf("frontend-web/package.json version = %q, want %q", got, want)
	}

	changelog, err := os.ReadFile("CHANGELOG.md")
	if err != nil {
		t.Fatalf("read CHANGELOG.md: %v", err)
	}
	var latestHeader string
	for _, line := range strings.Split(string(changelog), "\n") {
		if strings.HasPrefix(line, "## ") {
			latestHeader = strings.TrimSpace(line)
			break
		}
	}
	if latestHeader == "" {
		t.Fatal("CHANGELOG.md is missing a versioned heading")
	}
	if !strings.Contains(latestHeader, version) {
		t.Fatalf("latest changelog heading %q does not include %q", latestHeader, version)
	}
	if !strings.Contains(latestHeader, " - ") {
		t.Fatalf("latest changelog heading %q should include version and date", latestHeader)
	}
}
