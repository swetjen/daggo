package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

const defaultDSN = "file:daggo.sqlite?cache=shared&mode=rwc"

//go:embed sql/sqlite/schemas/*.sql
var schemaFS embed.FS

func Open(ctx context.Context, dsn string) (*Queries, *sql.DB, error) {
	if strings.TrimSpace(dsn) == "" {
		dsn = defaultDSN
	}
	if err := ensureSQLiteParentDir(dsn); err != nil {
		return nil, nil, err
	}

	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, nil, err
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	if err := ensureSchema(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA journal_mode = WAL;"); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout = 5000;"); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON;"); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	slog.Info("daggo: sqlite ready", "dsn", dsn)
	return New(conn), conn, nil
}

func ensureSchema(ctx context.Context, conn *sql.DB) error {
	if err := ensureMigrationsTable(ctx, conn); err != nil {
		return err
	}
	paths, err := fs.Glob(schemaFS, "sql/sqlite/schemas/*.sql")
	if err != nil {
		return fmt.Errorf("list schemas: %w", err)
	}
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)

	applied, err := loadAppliedMigrations(ctx, conn)
	if err != nil {
		return err
	}

	for _, path := range paths {
		if applied[path] {
			continue
		}
		data, err := schemaFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read schema %s: %w", path, err)
		}
		if _, err := conn.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("apply schema %s: %w", path, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (name) VALUES (?)", path); err != nil {
			return fmt.Errorf("record migration %s: %w", path, err)
		}
	}
	return nil
}

func ensureMigrationsTable(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, conn *sql.DB) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, "SELECT name FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("load migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan migration: %w", err)
		}
		applied[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return applied, nil
}

func ensureSQLiteParentDir(dsn string) error {
	path, ok, err := sqliteFilePath(dsn)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create sqlite parent dir %s: %w", dir, err)
	}
	return nil
}

func sqliteFilePath(dsn string) (string, bool, error) {
	trimmed := strings.TrimSpace(dsn)
	switch {
	case trimmed == "", trimmed == ":memory:":
		return "", false, nil
	case strings.HasPrefix(trimmed, "file://"):
		parsed, err := url.Parse(trimmed)
		if err != nil {
			return "", false, fmt.Errorf("parse sqlite dsn %q: %w", dsn, err)
		}
		if strings.EqualFold(parsed.Query().Get("mode"), "memory") || parsed.Path == "" || parsed.Path == "/:memory:" {
			return "", false, nil
		}
		return parsed.Path, true, nil
	case strings.HasPrefix(trimmed, "file:"):
		filename := strings.TrimPrefix(trimmed, "file:")
		path, query, _ := strings.Cut(filename, "?")
		if strings.EqualFold(queryValue(query, "mode"), "memory") || path == "" || path == ":memory:" {
			return "", false, nil
		}
		return path, true, nil
	default:
		return trimmed, true, nil
	}
}

func queryValue(rawQuery string, key string) string {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	return values.Get(key)
}
