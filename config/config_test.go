package config

import "testing"

func TestDefaultConfigUsesSQLite(t *testing.T) {
	cfg := Default()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate default config: %v", err)
	}
	if cfg.Database.Driver != DatabaseDriverSQLite {
		t.Fatalf("expected sqlite driver, got %q", cfg.Database.Driver)
	}
	if got := cfg.Database.SQLiteDSN(); got != "file:daggo.sqlite?cache=shared&mode=rwc" {
		t.Fatalf("unexpected sqlite dsn %q", got)
	}
	if cfg.DisableUI {
		t.Fatalf("expected UI to be enabled by default")
	}
	if cfg.Admin.SecretKey != "" {
		t.Fatalf("expected admin secret to be empty by default")
	}
}

func TestPostgresConfigValidationRequiresConnectionFields(t *testing.T) {
	cfg := Default()
	cfg.Database.Driver = DatabaseDriverPostgres
	cfg.Database.Postgres = PostgresConfig{}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected postgres config validation to fail")
	}
}

func TestLoadDoesNotEnablePostgresWithoutExplicitDriver(t *testing.T) {
	t.Setenv("DAGGO_POSTGRES_HOST", "db.internal")
	t.Setenv("DAGGO_POSTGRES_USER", "daggo")
	t.Setenv("DAGGO_POSTGRES_DATABASE", "platform")
	t.Setenv("DAGGO_POSTGRES_SCHEMA", "tenant_a")
	t.Setenv("DAGGO_POSTGRES_PASSWORD", "secret")
	t.Setenv("DAGGO_POSTGRES_PORT", "5432")

	cfg := Load()

	if cfg.Database.Driver != DatabaseDriverSQLite {
		t.Fatalf("expected sqlite driver by default, got %q", cfg.Database.Driver)
	}
}

func TestLoadUsesPostgresForPostgresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://daggo:secret@db.internal:5432/platform?sslmode=disable")

	cfg := Load()

	if cfg.Database.Driver != DatabaseDriverPostgres {
		t.Fatalf("expected postgres driver, got %q", cfg.Database.Driver)
	}
	if cfg.Database.Postgres.Host != "db.internal" {
		t.Fatalf("expected postgres host from database url, got %q", cfg.Database.Postgres.Host)
	}
	if cfg.Database.Postgres.Port != 5432 {
		t.Fatalf("expected postgres port 5432, got %d", cfg.Database.Postgres.Port)
	}
	if cfg.Database.Postgres.User != "daggo" {
		t.Fatalf("expected postgres user from database url, got %q", cfg.Database.Postgres.User)
	}
	if cfg.Database.Postgres.Password != "secret" {
		t.Fatalf("expected postgres password from database url, got %q", cfg.Database.Postgres.Password)
	}
	if cfg.Database.Postgres.Database != "platform" {
		t.Fatalf("expected postgres database from database url, got %q", cfg.Database.Postgres.Database)
	}
	if cfg.Database.Postgres.Schema != "daggo" {
		t.Fatalf("expected default postgres schema, got %q", cfg.Database.Postgres.Schema)
	}
	if cfg.Database.Postgres.SSLMode != "disable" {
		t.Fatalf("expected postgres sslmode from database url, got %q", cfg.Database.Postgres.SSLMode)
	}
}

func TestLoadUsesPGFallbacksWhenPostgresSelected(t *testing.T) {
	t.Setenv("DATABASE_DRIVER", "postgres")
	t.Setenv("PG_HOST", "db.internal")
	t.Setenv("PG_PORT", "5432")
	t.Setenv("PG_USER", "daggo")
	t.Setenv("PG_PASS", "secret")
	t.Setenv("PG_DB", "platform")
	t.Setenv("PG_SSLMODE", "disable")

	cfg := Load()

	if cfg.Database.Driver != DatabaseDriverPostgres {
		t.Fatalf("expected postgres driver, got %q", cfg.Database.Driver)
	}
	if cfg.Database.Postgres.Host != "db.internal" {
		t.Fatalf("expected postgres host from PG_HOST, got %q", cfg.Database.Postgres.Host)
	}
	if cfg.Database.Postgres.Port != 5432 {
		t.Fatalf("expected postgres port 5432, got %d", cfg.Database.Postgres.Port)
	}
	if cfg.Database.Postgres.User != "daggo" {
		t.Fatalf("expected postgres user from PG_USER, got %q", cfg.Database.Postgres.User)
	}
	if cfg.Database.Postgres.Password != "secret" {
		t.Fatalf("expected postgres password from PG_PASS, got %q", cfg.Database.Postgres.Password)
	}
	if cfg.Database.Postgres.Database != "platform" {
		t.Fatalf("expected postgres database from PG_DB, got %q", cfg.Database.Postgres.Database)
	}
	if cfg.Database.Postgres.Schema != "daggo" {
		t.Fatalf("expected default postgres schema, got %q", cfg.Database.Postgres.Schema)
	}
	if cfg.Database.Postgres.SSLMode != "disable" {
		t.Fatalf("expected postgres sslmode from PG_SSLMODE, got %q", cfg.Database.Postgres.SSLMode)
	}
}

func TestLoadCanDisableUI(t *testing.T) {
	t.Setenv("DAGGO_DISABLE_UI", "true")

	cfg := Load()

	if !cfg.DisableUI {
		t.Fatalf("expected UI to be disabled from env")
	}
}

func TestLoadCanSetAdminSecretKey(t *testing.T) {
	t.Setenv("DAGGO_ADMIN_SECRET_KEY", "test-secret")

	cfg := Load()

	if cfg.Admin.SecretKey != "test-secret" {
		t.Fatalf("expected admin secret key from env, got %q", cfg.Admin.SecretKey)
	}
}
