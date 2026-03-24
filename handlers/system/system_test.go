package system

import (
	"context"
	"testing"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

func TestInfoGetReturnsVersion(t *testing.T) {
	t.Parallel()

	handler := New(&deps.Deps{Version: "v0.5.0"})
	resp, status := handler.InfoGet(context.Background())
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if resp.Version != "v0.5.0" {
		t.Fatalf("expected version v0.5.0, got %q", resp.Version)
	}
}

func TestSettingsGetSanitizesSecretsAndNormalizesConfig(t *testing.T) {
	t.Parallel()

	handler := New(&deps.Deps{
		Version: "v0.5.0",
		Config: config.Config{
			Admin: config.AdminConfig{
				Port:      "9000",
				SecretKey: "super-secret",
			},
			AllowedOrigins: []string{"https://example.com", "https://ops.example.com"},
			Database: config.DatabaseConfig{
				Driver: config.DatabaseDriverPostgres,
				SQLite: config.SQLiteConfig{
					Path: "/tmp/daggo.sqlite",
					DSN:  "file:secret.sqlite",
				},
				Postgres: config.PostgresConfig{
					Host:     "db.internal",
					Port:     5433,
					User:     "daggo",
					Password: "db-password",
					Database: "daggo_prod",
					Schema:   "daggo_runtime",
					SSLMode:  "verify-full",
				},
			},
			Execution: config.ExecutionConfig{
				Mode:               "subprocess",
				QueueSize:          256,
				MaxConcurrentRuns:  3,
				MaxConcurrentSteps: 12,
			},
			Scheduler: config.SchedulerConfig{
				Enabled:       true,
				Key:           "primary",
				TickSeconds:   5,
				MaxDuePerTick: 42,
			},
			Deploy: config.DeployConfig{
				LockPath:          "runtime/WILL_DEPLOY",
				PollSeconds:       2,
				DrainGraceSeconds: 600,
			},
		},
	})

	resp, status := handler.SettingsGet(context.Background())
	if status != rpc.StatusOK {
		t.Fatalf("expected ok status, got %d", status)
	}
	if resp.Settings.Version != "v0.5.0" {
		t.Fatalf("expected version v0.5.0, got %q", resp.Settings.Version)
	}
	if resp.Settings.Admin.ListenAddr != ":9000" {
		t.Fatalf("expected listen addr :9000, got %q", resp.Settings.Admin.ListenAddr)
	}
	if !resp.Settings.Admin.SecretConfigured {
		t.Fatal("expected admin secret to be reported as configured")
	}
	if resp.Settings.Database.Driver != "postgres" {
		t.Fatalf("expected postgres driver, got %q", resp.Settings.Database.Driver)
	}
	if !resp.Settings.Database.SQLite.DSNConfigured {
		t.Fatal("expected sqlite dsn to be reported as configured")
	}
	if resp.Settings.Database.Postgres.Host != "db.internal" {
		t.Fatalf("expected postgres host db.internal, got %q", resp.Settings.Database.Postgres.Host)
	}
	if !resp.Settings.Database.Postgres.PasswordConfigured {
		t.Fatal("expected postgres password to be reported as configured")
	}
	if resp.Settings.Execution.MaxConcurrentRuns != 3 {
		t.Fatalf("expected max concurrent runs 3, got %d", resp.Settings.Execution.MaxConcurrentRuns)
	}
	if len(resp.Settings.AllowedOrigins) != 2 {
		t.Fatalf("expected 2 allowed origins, got %d", len(resp.Settings.AllowedOrigins))
	}
}
