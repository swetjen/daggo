package db

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/swetjen/daggo/config"
)

func postgresTestDatabase(t *testing.T) config.DatabaseConfig {
	t.Helper()

	dsn := os.Getenv(TestPostgresDSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set", TestPostgresDSNEnv)
	}
	database, err := PostgresTestConfig(dsn, NewTestPostgresSchemaName())
	if err != nil {
		t.Fatalf("postgres test config: %v", err)
	}
	t.Cleanup(func() {
		if err := DropTestPostgresSchema(context.Background(), database); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	return database
}

func appliedPostgresMigrationNames(t *testing.T, pool *sql.DB) []string {
	t.Helper()

	rows, err := pool.QueryContext(context.Background(), "SELECT name FROM schema_migrations ORDER BY name")
	if err != nil {
		t.Fatalf("load applied migrations: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan migration: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("migration rows: %v", err)
	}
	return names
}

func TestOpenRuntimePostgresConcurrentStartsOnEmptySchemaAllSucceed(t *testing.T) {
	database := postgresTestDatabase(t)

	const starters = 8
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := make(chan struct{})
	errs := make([]error, starters)
	pools := make([]*sql.DB, starters)
	var wg sync.WaitGroup
	for idx := 0; idx < starters; idx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, pool, err := OpenRuntime(ctx, database)
			errs[idx] = err
			pools[idx] = pool
		}(idx)
	}
	close(start)
	wg.Wait()

	t.Cleanup(func() {
		for _, pool := range pools {
			if pool != nil {
				_ = pool.Close()
			}
		}
	})
	for idx, err := range errs {
		if err != nil {
			t.Errorf("starter %d failed: %v", idx, err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	want, err := fs.Glob(postgresSchemaFS, "sql/postgres/schemas/*.sql")
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	sort.Strings(want)
	got := appliedPostgresMigrationNames(t, pools[0])
	if len(got) != len(want) {
		t.Fatalf("applied migrations = %v, want %v", got, want)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("applied migrations = %v, want %v", got, want)
		}
	}

	// The schema every starter ended up with is usable.
	store := NewPostgresStore(pools[starters-1])
	if _, err := store.JobCount(ctx); err != nil {
		t.Fatalf("query migrated schema: %v", err)
	}
}

func TestOpenRuntimePostgresRestartOnMigratedSchemaIsNoop(t *testing.T) {
	database := postgresTestDatabase(t)
	ctx := context.Background()

	_, first, err := OpenRuntime(ctx, database)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	before := appliedPostgresMigrationNames(t, first)

	_, second, err := OpenRuntime(ctx, database)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	after := appliedPostgresMigrationNames(t, second)

	if len(before) == 0 || len(before) != len(after) {
		t.Fatalf("migrations changed across restart: before=%v after=%v", before, after)
	}
}

func TestPostgresMigrationLockKeyIsStablePerSchema(t *testing.T) {
	t.Parallel()

	if postgresMigrationLockKey("daggo") != postgresMigrationLockKey("daggo") {
		t.Fatalf("lock key must be stable for a schema")
	}
	if postgresMigrationLockKey("daggo") == postgresMigrationLockKey("other") {
		t.Fatalf("different schemas must not share a lock key")
	}
}
