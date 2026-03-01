package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"net/url"
	"regexp"
	"sort"
	"strings"

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

	bootstrapDSN := postgresDSN(pg, false)
	bootstrapConn, err := sql.Open("pgx", bootstrapDSN)
	if err != nil {
		return nil, nil, err
	}
	defer bootstrapConn.Close()

	if err := bootstrapConn.PingContext(ctx); err != nil {
		return nil, nil, err
	}
	if _, err := bootstrapConn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quotePostgresIdentifier(pg.Schema)); err != nil {
		return nil, nil, fmt.Errorf("create postgres schema %s: %w", pg.Schema, err)
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
	if err := ensurePostgresSchema(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return NewPostgresStore(conn), conn, nil
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
		if _, err := conn.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("apply postgres schema %s: %w", path, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (name) VALUES ($1)", path); err != nil {
			return fmt.Errorf("record postgres migration %s: %w", path, err)
		}
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
