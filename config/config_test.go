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

func TestMaxConcurrentRunsDefaultsDependOnExecutionMode(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		configured int
		want       int
	}{
		{name: "subprocess unset uses subprocess default", mode: "subprocess", configured: 0, want: DefaultSubprocessMaxConcurrentRuns},
		{name: "default mode unset uses subprocess default", mode: "", configured: 0, want: DefaultSubprocessMaxConcurrentRuns},
		{name: "in_process unset stays serial", mode: "in_process", configured: 0, want: DefaultInProcessMaxConcurrentRuns},
		{name: "subprocess explicit one is honored", mode: "subprocess", configured: 1, want: 1},
		{name: "subprocess explicit value is honored", mode: "subprocess", configured: 3, want: 3},
		{name: "in_process explicit value is honored", mode: "in_process", configured: 4, want: 4},
		{name: "negative is treated as unset", mode: "subprocess", configured: -2, want: DefaultSubprocessMaxConcurrentRuns},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Execution.Mode = tt.mode
			cfg.Execution.MaxConcurrentRuns = tt.configured

			normalized := cfg.Normalized()
			if got := normalized.Execution.MaxConcurrentRuns; got != tt.want {
				t.Fatalf("MaxConcurrentRuns = %d, want %d", got, tt.want)
			}
			if got := normalized.Normalized().Execution.MaxConcurrentRuns; got != tt.want {
				t.Fatalf("normalizing twice changed MaxConcurrentRuns to %d, want %d", got, tt.want)
			}
		})
	}

	if DefaultSubprocessMaxConcurrentRuns != 8 {
		t.Fatalf("subprocess default changed to %d; update the changelog and docs", DefaultSubprocessMaxConcurrentRuns)
	}
	if DefaultInProcessMaxConcurrentRuns != 1 {
		t.Fatalf("in_process default changed to %d; update the changelog and docs", DefaultInProcessMaxConcurrentRuns)
	}
}

func TestLoadLeavesMaxConcurrentRunsUnsetWithoutEnv(t *testing.T) {
	t.Setenv("RUN_MAX_CONCURRENT_RUNS", "")
	t.Setenv("RUN_EXECUTION_MODE", "")

	cfg := Load()

	if cfg.Execution.MaxConcurrentRuns != 0 {
		t.Fatalf("expected unset cap to stay unset after Load, got %d", cfg.Execution.MaxConcurrentRuns)
	}
	if got := cfg.Normalized().Execution.MaxConcurrentRuns; got != DefaultSubprocessMaxConcurrentRuns {
		t.Fatalf("expected subprocess default %d, got %d", DefaultSubprocessMaxConcurrentRuns, got)
	}

	// Switching the mode in code after Load must not inherit the
	// subprocess default.
	cfg.Execution.Mode = "in_process"
	if got := cfg.Normalized().Execution.MaxConcurrentRuns; got != DefaultInProcessMaxConcurrentRuns {
		t.Fatalf("expected in_process default %d, got %d", DefaultInProcessMaxConcurrentRuns, got)
	}
}

func TestLoadHonorsExplicitMaxConcurrentRuns(t *testing.T) {
	t.Setenv("RUN_EXECUTION_MODE", "subprocess")
	t.Setenv("RUN_MAX_CONCURRENT_RUNS", "1")

	cfg := Load()

	if cfg.Execution.MaxConcurrentRuns != 1 {
		t.Fatalf("expected explicit cap 1 from env, got %d", cfg.Execution.MaxConcurrentRuns)
	}
	if got := cfg.Normalized().Execution.MaxConcurrentRuns; got != 1 {
		t.Fatalf("expected explicit cap 1 to survive normalization, got %d", got)
	}
}

func TestLoadUsesInProcessDefaultWhenModeComesFromEnv(t *testing.T) {
	t.Setenv("RUN_EXECUTION_MODE", "in_process")
	t.Setenv("RUN_MAX_CONCURRENT_RUNS", "")

	cfg := Load()

	if got := cfg.Normalized().Execution.MaxConcurrentRuns; got != DefaultInProcessMaxConcurrentRuns {
		t.Fatalf("expected in_process default %d, got %d", DefaultInProcessMaxConcurrentRuns, got)
	}
}
