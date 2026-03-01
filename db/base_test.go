package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesSQLiteParentDir(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "runtime", "daggo.sqlite")

	queries, pool, err := Open(context.Background(), "file:"+dbPath+"?cache=shared&mode=rwc")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	if queries == nil {
		t.Fatalf("expected queries")
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("stat sqlite db: %v", err)
	}
}
