package system

import (
	"context"

	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/virtuous/rpc"
)

type Handlers struct {
	app *deps.Deps
}

func New(app *deps.Deps) *Handlers {
	return &Handlers{app: app}
}

type InfoGetResponse struct {
	Version string `json:"version"`
	Error   string `json:"error,omitempty"`
}

type SettingsAdmin struct {
	Port             string `json:"port"`
	ListenAddr       string `json:"listen_addr"`
	UIEnabled        bool   `json:"ui_enabled"`
	SecretConfigured bool   `json:"secret_configured"`
}

type SettingsDatabaseSQLite struct {
	Path          string `json:"path"`
	DSNConfigured bool   `json:"dsn_configured"`
}

type SettingsDatabasePostgres struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	User               string `json:"user"`
	Database           string `json:"database"`
	Schema             string `json:"schema"`
	SSLMode            string `json:"sslmode"`
	PasswordConfigured bool   `json:"password_configured"`
}

type SettingsDatabase struct {
	Driver   string                   `json:"driver"`
	SQLite   SettingsDatabaseSQLite   `json:"sqlite"`
	Postgres SettingsDatabasePostgres `json:"postgres"`
}

type SettingsExecution struct {
	Mode               string `json:"mode"`
	QueueSize          int    `json:"queue_size"`
	MaxConcurrentRuns  int    `json:"max_concurrent_runs"`
	MaxConcurrentSteps int    `json:"max_concurrent_steps"`
}

type SettingsScheduler struct {
	Enabled       bool   `json:"enabled"`
	Key           string `json:"key"`
	TickSeconds   int    `json:"tick_seconds"`
	MaxDuePerTick int    `json:"max_due_per_tick"`
}

type SettingsDeploy struct {
	LockPath          string `json:"lock_path"`
	PollSeconds       int    `json:"poll_seconds"`
	DrainGraceSeconds int    `json:"drain_grace_seconds"`
}

type SettingsSnapshot struct {
	Version          string            `json:"version"`
	AllowedOrigins   []string          `json:"allowed_origins"`
	RegisteredJobs   int               `json:"registered_jobs"`
	RegisteredQueues int               `json:"registered_queues"`
	Admin            SettingsAdmin     `json:"admin"`
	Database         SettingsDatabase  `json:"database"`
	Execution        SettingsExecution `json:"execution"`
	Scheduler        SettingsScheduler `json:"scheduler"`
	Deploy           SettingsDeploy    `json:"deploy"`
}

type SettingsGetResponse struct {
	Settings SettingsSnapshot `json:"settings"`
	Error    string           `json:"error,omitempty"`
}

func (h *Handlers) InfoGet(_ context.Context) (InfoGetResponse, int) {
	version := ""
	if h != nil && h.app != nil {
		version = h.app.Version
	}
	return InfoGetResponse{Version: version}, rpc.StatusOK
}

func (h *Handlers) SettingsGet(_ context.Context) (SettingsGetResponse, int) {
	if h == nil || h.app == nil {
		return SettingsGetResponse{Settings: SettingsSnapshot{}}, rpc.StatusOK
	}

	cfg := h.app.Config.Normalized()
	return SettingsGetResponse{
		Settings: SettingsSnapshot{
			Version:          h.app.Version,
			AllowedOrigins:   append([]string(nil), cfg.AllowedOrigins...),
			RegisteredJobs:   registeredJobs(h.app),
			RegisteredQueues: registeredQueues(h.app),
			Admin: SettingsAdmin{
				Port:             cfg.Admin.Port,
				ListenAddr:       cfg.ListenAddr(),
				UIEnabled:        !cfg.DisableUI,
				SecretConfigured: cfg.Admin.SecretKey != "",
			},
			Database: SettingsDatabase{
				Driver: string(cfg.Database.Driver),
				SQLite: SettingsDatabaseSQLite{
					Path:          cfg.Database.SQLite.Path,
					DSNConfigured: cfg.Database.SQLite.DSN != "",
				},
				Postgres: SettingsDatabasePostgres{
					Host:               cfg.Database.Postgres.Host,
					Port:               cfg.Database.Postgres.Port,
					User:               cfg.Database.Postgres.User,
					Database:           cfg.Database.Postgres.Database,
					Schema:             cfg.Database.Postgres.Schema,
					SSLMode:            cfg.Database.Postgres.SSLMode,
					PasswordConfigured: cfg.Database.Postgres.Password != "",
				},
			},
			Execution: SettingsExecution{
				Mode:               cfg.Execution.Mode,
				QueueSize:          cfg.Execution.QueueSize,
				MaxConcurrentRuns:  cfg.Execution.MaxConcurrentRuns,
				MaxConcurrentSteps: cfg.Execution.MaxConcurrentSteps,
			},
			Scheduler: SettingsScheduler{
				Enabled:       cfg.Scheduler.Enabled,
				Key:           cfg.Scheduler.Key,
				TickSeconds:   cfg.Scheduler.TickSeconds,
				MaxDuePerTick: cfg.Scheduler.MaxDuePerTick,
			},
			Deploy: SettingsDeploy{
				LockPath:          cfg.Deploy.LockPath,
				PollSeconds:       cfg.Deploy.PollSeconds,
				DrainGraceSeconds: cfg.Deploy.DrainGraceSeconds,
			},
		},
	}, rpc.StatusOK
}

func registeredJobs(app *deps.Deps) int {
	if app == nil || app.Registry == nil {
		return 0
	}
	return len(app.Registry.Jobs())
}

func registeredQueues(app *deps.Deps) int {
	if app == nil || app.Queues == nil {
		return 0
	}
	return len(app.Queues.Queues())
}
