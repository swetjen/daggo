package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"hash/fnv"
	"io/fs"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/swetjen/daggo/config"
)

var postgresSchemaNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

//go:embed sql/postgres/schemas/*.sql
var postgresSchemaFS embed.FS

func OpenRuntime(ctx context.Context, database config.DatabaseConfig) (Store, *sql.DB, error) {
	cfg := database.Normalized()
	switch cfg.Driver {
	case config.DatabaseDriverSQLite:
		return Open(ctx, cfg.SQLiteDSN())
	case config.DatabaseDriverPostgres:
		return openPostgres(ctx, cfg)
	default:
		return nil, nil, fmt.Errorf("unsupported database driver %q", cfg.Driver)
	}
}

func openPostgres(ctx context.Context, database config.DatabaseConfig) (Store, *sql.DB, error) {
	pg := database.Postgres
	if !postgresSchemaNamePattern.MatchString(pg.Schema) {
		return nil, nil, fmt.Errorf("invalid postgres schema %q", pg.Schema)
	}

	runtimeDSN := postgresDSN(pg, true)
	conn, err := sql.Open("pgx", runtimeDSN)
	if err != nil {
		return nil, nil, err
	}
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(4)

	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	// A process that finds the schema already current, which is every worker
	// and every restart without a new release, starts without taking the
	// migration lock.
	if !postgresSchemaIsCurrent(ctx, conn, pg.Schema) {
		if err := migratePostgresSchema(ctx, pg, conn); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
	}
	return NewPostgresStore(conn), conn, nil
}

// migratePostgresSchema creates the schema and applies pending migrations
// while holding a PostgreSQL advisory lock, so processes that start at the
// same time take turns instead of racing each other on the same DDL. The lock
// is scoped to the schema name and is held on its own connection; it is
// released when migrations finish or that connection ends.
func migratePostgresSchema(ctx context.Context, pg config.PostgresConfig, conn *sql.DB) error {
	bootstrapPool, err := sql.Open("pgx", postgresDSN(pg, false))
	if err != nil {
		return err
	}
	defer bootstrapPool.Close()

	lockConn, err := bootstrapPool.Conn(ctx)
	if err != nil {
		return err
	}
	defer lockConn.Close()

	lockKey := postgresMigrationLockKey(pg.Schema)
	if _, err := lockConn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		return fmt.Errorf("acquire postgres migration lock for schema %s: %w", pg.Schema, err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = lockConn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", lockKey)
	}()

	if _, err := lockConn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quotePostgresIdentifier(pg.Schema)); err != nil {
		return fmt.Errorf("create postgres schema %s: %w", pg.Schema, err)
	}
	return ensurePostgresSchema(ctx, conn)
}

// postgresMigrationLockKey derives the advisory lock key for a schema.
func postgresMigrationLockKey(schema string) int64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte("daggo:schema_migrations:" + schema))
	return int64(hasher.Sum64())
}

// postgresSchemaIsCurrent reports whether every bundled migration is already
// recorded for the schema. Any failure, including a schema or migrations table
// that does not exist yet, reports false so the locked path decides.
func postgresSchemaIsCurrent(ctx context.Context, conn *sql.DB, schema string) bool {
	paths, err := fs.Glob(postgresSchemaFS, "sql/postgres/schemas/*.sql")
	if err != nil || len(paths) == 0 {
		return false
	}
	rows, err := conn.QueryContext(ctx, "SELECT name FROM "+quotePostgresIdentifier(schema)+".schema_migrations")
	if err != nil {
		return false
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		applied[name] = true
	}
	if err := rows.Err(); err != nil {
		return false
	}
	for _, path := range paths {
		if !applied[path] {
			return false
		}
	}
	return true
}

func postgresDSN(cfg config.PostgresConfig, includeSearchPath bool) string {
	query := url.Values{}
	if strings.TrimSpace(cfg.SSLMode) != "" {
		query.Set("sslmode", strings.TrimSpace(cfg.SSLMode))
	}
	if includeSearchPath {
		query.Set("options", "-c search_path="+cfg.Schema+",public")
	}

	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     cfg.Database,
		RawQuery: query.Encode(),
	}).String()
}

func ensurePostgresSchema(ctx context.Context, conn *sql.DB) error {
	if err := ensurePostgresMigrationsTable(ctx, conn); err != nil {
		return err
	}
	paths, err := fs.Glob(postgresSchemaFS, "sql/postgres/schemas/*.sql")
	if err != nil {
		return fmt.Errorf("list postgres schemas: %w", err)
	}
	sort.Strings(paths)

	applied, err := loadAppliedPostgresMigrations(ctx, conn)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if applied[path] {
			continue
		}
		data, err := postgresSchemaFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read postgres schema %s: %w", path, err)
		}
		if err := applyPostgresMigration(ctx, conn, path, string(data)); err != nil {
			return err
		}
	}
	return nil
}

// applyPostgresMigration applies one migration and records it in the same
// transaction, so a migration is never left applied but unrecorded.
func applyPostgresMigration(ctx context.Context, conn *sql.DB, path string, statements string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin postgres migration %s: %w", path, err)
	}
	if _, err := tx.ExecContext(ctx, statements); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("apply postgres schema %s: %w", path, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO schema_migrations (name) VALUES ($1)", path); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record postgres migration %s: %w", path, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres migration %s: %w", path, err)
	}
	return nil
}

func ensurePostgresMigrationsTable(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	);`)
	if err != nil {
		return fmt.Errorf("create postgres migrations table: %w", err)
	}
	return nil
}

func loadAppliedPostgresMigrations(ctx context.Context, conn *sql.DB) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT name FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("load postgres migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan postgres migration: %w", err)
		}
		applied[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres migration rows: %w", err)
	}
	return applied, nil
}

func quotePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
