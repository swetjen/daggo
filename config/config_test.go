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

func TestLoadCanDisableUI(t *testing.T) {
	t.Setenv("DAGGO_DISABLE_UI", "true")

	cfg := Load()

	if !cfg.DisableUI {
		t.Fatalf("expected UI to be disabled from env")
	}
}
