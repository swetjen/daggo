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
	if cfg.Retention.RunDays != 0 {
		t.Fatalf("expected run retention to be disabled by default, got %d", cfg.Retention.RunDays)
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

func TestLoadCanSetAdminSecretKey(t *testing.T) {
	t.Setenv("DAGGO_ADMIN_SECRET_KEY", "test-secret")

	cfg := Load()

	if cfg.Admin.SecretKey != "test-secret" {
		t.Fatalf("expected admin secret key from env, got %q", cfg.Admin.SecretKey)
	}
}

func TestLoadCanSetRunRetentionDays(t *testing.T) {
	t.Setenv("RUN_RETENTION_DAYS", "45")

	cfg := Load()

	if cfg.Retention.RunDays != 45 {
		t.Fatalf("expected run retention days 45 from env, got %d", cfg.Retention.RunDays)
	}
}

func TestLoadCanDisableRunRetentionWithZero(t *testing.T) {
	t.Setenv("RUN_RETENTION_DAYS", "0")

	cfg := Load()

	if cfg.Retention.RunDays != 0 {
		t.Fatalf("expected run retention days 0 from env, got %d", cfg.Retention.RunDays)
	}
}
