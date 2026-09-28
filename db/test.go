package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/swetjen/daggo/config"
)

// TestPostgresDSNEnv names the environment variable that points
// PostgreSQL-only tests at a throwaway server, for example
// postgres://postgres:test@127.0.0.1:55437/postgres?sslmode=disable.
// Tests that need PostgreSQL skip when it is unset.
const TestPostgresDSNEnv = "DAGGO_TEST_POSTGRES_DSN"

func NewTest() (*Queries, *sql.DB, error) {
	dsn := fmt.Sprintf("file:daggo-test-%d?mode=memory&cache=shared", time.Now().UnixNano())
	return Open(context.Background(), dsn)
}

// PostgresTestConfig builds a database config for the server named by dsn,
// using the given schema.
func PostgresTestConfig(dsn string, schema string) (config.DatabaseConfig, error) {
	parsed, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil {
		return config.DatabaseConfig{}, fmt.Errorf("parse postgres test dsn: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return config.DatabaseConfig{}, fmt.Errorf("postgres test dsn must use the postgres:// scheme")
	}
	port := 5432
	if raw := parsed.Port(); raw != "" {
		port, err = strconv.Atoi(raw)
		if err != nil {
			return config.DatabaseConfig{}, fmt.Errorf("parse postgres test dsn port: %w", err)
		}
	}
	password, _ := parsed.User.Password()
	sslMode := parsed.Query().Get("sslmode")
	if sslMode == "" {
		sslMode = "disable"
	}
	return config.DatabaseConfig{
		Driver: config.DatabaseDriverPostgres,
		Postgres: config.PostgresConfig{
			Host:     parsed.Hostname(),
			Port:     port,
			User:     parsed.User.Username(),
			Password: password,
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Schema:   schema,
			SSLMode:  sslMode,
		},
	}, nil
}

// NewTestPostgresSchemaName returns a schema name that is unique to one test.
func NewTestPostgresSchemaName() string {
	return fmt.Sprintf("daggo_test_%d", time.Now().UnixNano())
}

// DropTestPostgresSchema removes a schema created for a test.
func DropTestPostgresSchema(ctx context.Context, database config.DatabaseConfig) error {
	cfg := database.Normalized()
	if !postgresSchemaNamePattern.MatchString(cfg.Postgres.Schema) {
		return fmt.Errorf("invalid postgres schema %q", cfg.Postgres.Schema)
	}
	conn, err := sql.Open("pgx", postgresDSN(cfg.Postgres, false))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quotePostgresIdentifier(cfg.Postgres.Schema)+" CASCADE")
	return err
}

// NewTestPostgres opens a migrated store in a fresh schema on the server named
// by dsn. The returned cleanup closes the pool and drops the schema.
func NewTestPostgres(dsn string) (Store, *sql.DB, func(), error) {
	database, err := PostgresTestConfig(dsn, NewTestPostgresSchemaName())
	if err != nil {
		return nil, nil, nil, err
	}
	ctx := context.Background()
	store, pool, err := OpenRuntime(ctx, database)
	if err != nil {
		_ = DropTestPostgresSchema(ctx, database)
		return nil, nil, nil, err
	}
	cleanup := func() {
		_ = pool.Close()
		_ = DropTestPostgresSchema(context.Background(), database)
	}
	return store, pool, cleanup, nil
}
