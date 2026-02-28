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
}

func TestPostgresConfigValidationRequiresConnectionFields(t *testing.T) {
	cfg := Default()
	cfg.Database.Driver = DatabaseDriverPostgres
	cfg.Database.Postgres = PostgresConfig{}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected postgres config validation to fail")
	}
}
