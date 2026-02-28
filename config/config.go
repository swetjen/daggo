package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Admin          AdminConfig
	AllowedOrigins []string
	Database       DatabaseConfig
	Execution      ExecutionConfig
	Scheduler      SchedulerConfig
	Deploy         DeployConfig
}

type AdminConfig struct {
	Port string
}

type DatabaseDriver string

const (
	DatabaseDriverSQLite   DatabaseDriver = "sqlite"
	DatabaseDriverPostgres DatabaseDriver = "postgres"
)

type DatabaseConfig struct {
	Driver   DatabaseDriver
	SQLite   SQLiteConfig
	Postgres PostgresConfig
}

type SQLiteConfig struct {
	Path string
	DSN  string
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Schema   string
	SSLMode  string
}

type ExecutionConfig struct {
	QueueSize          int
	Mode               string
	MaxConcurrentRuns  int
	MaxConcurrentSteps int
}

type SchedulerConfig struct {
	Enabled       bool
	Key           string
	TickSeconds   int
	MaxDuePerTick int
}

type DeployConfig struct {
	LockPath          string
	PollSeconds       int
	DrainGraceSeconds int
}

func Default() Config {
	return Config{
		Admin: AdminConfig{
			Port: "8000",
		},
		AllowedOrigins: []string{"*"},
		Database: DatabaseConfig{
			Driver: DatabaseDriverSQLite,
			SQLite: SQLiteConfig{
				Path: "daggo.sqlite",
			},
			Postgres: PostgresConfig{
				Port:    5432,
				Schema:  "daggo",
				SSLMode: "require",
			},
		},
		Execution: ExecutionConfig{
			QueueSize:          128,
			Mode:               "subprocess",
			MaxConcurrentRuns:  1,
			MaxConcurrentSteps: 8,
		},
		Scheduler: SchedulerConfig{
			Enabled:       true,
			Key:           "monolith",
			TickSeconds:   15,
			MaxDuePerTick: 24,
		},
		Deploy: DeployConfig{
			LockPath:          "runtime/WILL_DEPLOY",
			PollSeconds:       1,
			DrainGraceSeconds: 900,
		},
	}
}

func Load() Config {
	loadDotEnv(".env")

	cfg := Default()
	cfg.Admin.Port = getEnvAny([]string{"DAGGO_ADMIN_PORT", "PORT"}, cfg.Admin.Port)
	cfg.AllowedOrigins = splitEnvListAny([]string{"DAGGO_ALLOWED_ORIGINS", "CORS_ALLOW_ORIGINS"}, cfg.AllowedOrigins)

	sqlitePath := getEnvAny([]string{"DAGGO_SQLITE_PATH"}, "")
	sqliteDSN := getEnvAny([]string{"DAGGO_SQLITE_DSN", "DATABASE_URL"}, "")
	driver := strings.ToLower(strings.TrimSpace(getEnvAny([]string{"DAGGO_DATABASE_DRIVER", "DATABASE_DRIVER"}, "")))
	if driver == "" {
		switch {
		case hasAnyEnv([]string{
			"DAGGO_POSTGRES_HOST",
			"DAGGO_POSTGRES_PORT",
			"DAGGO_POSTGRES_USER",
			"DAGGO_POSTGRES_PASSWORD",
			"DAGGO_POSTGRES_DATABASE",
			"DAGGO_POSTGRES_SCHEMA",
			"DAGGO_POSTGRES_SSLMODE",
		}):
			driver = string(DatabaseDriverPostgres)
		case sqlitePath != "" || sqliteDSN != "":
			driver = string(DatabaseDriverSQLite)
		default:
			driver = string(cfg.Database.Driver)
		}
	}

	cfg.Database.Driver = DatabaseDriver(driver)
	if sqlitePath != "" {
		cfg.Database.SQLite.Path = sqlitePath
	}
	if sqliteDSN != "" {
		cfg.Database.SQLite.DSN = sqliteDSN
	}
	cfg.Database.Postgres.Host = getEnvAny([]string{"DAGGO_POSTGRES_HOST"}, cfg.Database.Postgres.Host)
	cfg.Database.Postgres.Port = getEnvIntAny([]string{"DAGGO_POSTGRES_PORT"}, cfg.Database.Postgres.Port)
	cfg.Database.Postgres.User = getEnvAny([]string{"DAGGO_POSTGRES_USER"}, cfg.Database.Postgres.User)
	cfg.Database.Postgres.Password = getEnvAny([]string{"DAGGO_POSTGRES_PASSWORD"}, cfg.Database.Postgres.Password)
	cfg.Database.Postgres.Database = getEnvAny([]string{"DAGGO_POSTGRES_DATABASE"}, cfg.Database.Postgres.Database)
	cfg.Database.Postgres.Schema = getEnvAny([]string{"DAGGO_POSTGRES_SCHEMA"}, cfg.Database.Postgres.Schema)
	cfg.Database.Postgres.SSLMode = getEnvAny([]string{"DAGGO_POSTGRES_SSLMODE"}, cfg.Database.Postgres.SSLMode)

	cfg.Execution.QueueSize = getEnvIntAny([]string{"RUN_QUEUE_SIZE"}, cfg.Execution.QueueSize)
	cfg.Execution.Mode = getEnvAny([]string{"RUN_EXECUTION_MODE"}, cfg.Execution.Mode)
	cfg.Execution.MaxConcurrentRuns = getEnvIntAny([]string{"RUN_MAX_CONCURRENT_RUNS"}, cfg.Execution.MaxConcurrentRuns)
	cfg.Execution.MaxConcurrentSteps = getEnvIntAny([]string{"RUN_MAX_CONCURRENT_STEPS"}, cfg.Execution.MaxConcurrentSteps)

	cfg.Scheduler.Enabled = getEnvBoolAny([]string{"SCHEDULER_ENABLED"}, cfg.Scheduler.Enabled)
	cfg.Scheduler.Key = getEnvAny([]string{"SCHEDULER_KEY"}, cfg.Scheduler.Key)
	cfg.Scheduler.TickSeconds = getEnvIntAny([]string{"SCHEDULER_TICK_SECONDS"}, cfg.Scheduler.TickSeconds)
	cfg.Scheduler.MaxDuePerTick = getEnvIntAny([]string{"SCHEDULER_MAX_DUE_PER_TICK"}, cfg.Scheduler.MaxDuePerTick)

	cfg.Deploy.LockPath = getEnvAny([]string{"DEPLOY_LOCK_PATH"}, cfg.Deploy.LockPath)
	cfg.Deploy.PollSeconds = getEnvIntAny([]string{"DEPLOY_LOCK_POLL_SECONDS"}, cfg.Deploy.PollSeconds)
	cfg.Deploy.DrainGraceSeconds = getEnvIntAny([]string{"DEPLOY_DRAIN_GRACE_SECONDS"}, cfg.Deploy.DrainGraceSeconds)

	return cfg.Normalized()
}

func (c Config) Normalized() Config {
	defaults := Default()

	out := defaults
	out.Admin.Port = normalizePort(firstNonEmpty(c.Admin.Port, defaults.Admin.Port))

	if cleaned := cleanList(c.AllowedOrigins); len(cleaned) > 0 {
		out.AllowedOrigins = cleaned
	}

	if driver := normalizeDatabaseDriver(c.Database.Driver); driver != "" {
		out.Database.Driver = driver
	}
	if strings.TrimSpace(c.Database.SQLite.Path) != "" {
		out.Database.SQLite.Path = strings.TrimSpace(c.Database.SQLite.Path)
	}
	if strings.TrimSpace(c.Database.SQLite.DSN) != "" {
		out.Database.SQLite.DSN = strings.TrimSpace(c.Database.SQLite.DSN)
	}

	if strings.TrimSpace(c.Database.Postgres.Host) != "" {
		out.Database.Postgres.Host = strings.TrimSpace(c.Database.Postgres.Host)
	}
	if c.Database.Postgres.Port > 0 {
		out.Database.Postgres.Port = c.Database.Postgres.Port
	}
	if strings.TrimSpace(c.Database.Postgres.User) != "" {
		out.Database.Postgres.User = strings.TrimSpace(c.Database.Postgres.User)
	}
	if c.Database.Postgres.Password != "" {
		out.Database.Postgres.Password = c.Database.Postgres.Password
	}
	if strings.TrimSpace(c.Database.Postgres.Database) != "" {
		out.Database.Postgres.Database = strings.TrimSpace(c.Database.Postgres.Database)
	}
	if strings.TrimSpace(c.Database.Postgres.Schema) != "" {
		out.Database.Postgres.Schema = strings.TrimSpace(c.Database.Postgres.Schema)
	}
	if strings.TrimSpace(c.Database.Postgres.SSLMode) != "" {
		out.Database.Postgres.SSLMode = strings.TrimSpace(c.Database.Postgres.SSLMode)
	}

	if c.Execution.QueueSize > 0 {
		out.Execution.QueueSize = c.Execution.QueueSize
	}
	if strings.TrimSpace(c.Execution.Mode) != "" {
		out.Execution.Mode = strings.TrimSpace(c.Execution.Mode)
	}
	if c.Execution.MaxConcurrentRuns > 0 {
		out.Execution.MaxConcurrentRuns = c.Execution.MaxConcurrentRuns
	}
	if c.Execution.MaxConcurrentSteps > 0 {
		out.Execution.MaxConcurrentSteps = c.Execution.MaxConcurrentSteps
	}

	if c.Scheduler.Enabled || hasSchedulerOverrides(c.Scheduler) {
		out.Scheduler.Enabled = c.Scheduler.Enabled
	}
	if strings.TrimSpace(c.Scheduler.Key) != "" {
		out.Scheduler.Key = strings.TrimSpace(c.Scheduler.Key)
	}
	if c.Scheduler.TickSeconds > 0 {
		out.Scheduler.TickSeconds = c.Scheduler.TickSeconds
	}
	if c.Scheduler.MaxDuePerTick > 0 {
		out.Scheduler.MaxDuePerTick = c.Scheduler.MaxDuePerTick
	}

	if strings.TrimSpace(c.Deploy.LockPath) != "" {
		out.Deploy.LockPath = strings.TrimSpace(c.Deploy.LockPath)
	}
	if c.Deploy.PollSeconds > 0 {
		out.Deploy.PollSeconds = c.Deploy.PollSeconds
	}
	if c.Deploy.DrainGraceSeconds > 0 {
		out.Deploy.DrainGraceSeconds = c.Deploy.DrainGraceSeconds
	}

	return out
}

func (c Config) ListenAddr() string {
	normalized := c.Normalized()
	return ":" + normalizePort(normalized.Admin.Port)
}

func (c Config) Validate() error {
	cfg := c.Normalized()
	if normalizePort(cfg.Admin.Port) == "" {
		return fmt.Errorf("admin port is required")
	}

	switch cfg.Database.Driver {
	case DatabaseDriverSQLite:
		if strings.TrimSpace(cfg.Database.SQLite.DSN) == "" && strings.TrimSpace(cfg.Database.SQLite.Path) == "" {
			return fmt.Errorf("sqlite path or dsn is required")
		}
	case DatabaseDriverPostgres:
		if strings.TrimSpace(cfg.Database.Postgres.Host) == "" {
			return fmt.Errorf("postgres host is required")
		}
		if strings.TrimSpace(cfg.Database.Postgres.User) == "" {
			return fmt.Errorf("postgres user is required")
		}
		if strings.TrimSpace(cfg.Database.Postgres.Database) == "" {
			return fmt.Errorf("postgres database is required")
		}
		if strings.TrimSpace(cfg.Database.Postgres.Schema) == "" {
			return fmt.Errorf("postgres schema is required")
		}
	default:
		return fmt.Errorf("unsupported database driver %q", cfg.Database.Driver)
	}

	return nil
}

func (c DatabaseConfig) Normalized() DatabaseConfig {
	cfg := Default().Database
	if driver := normalizeDatabaseDriver(c.Driver); driver != "" {
		cfg.Driver = driver
	}
	if strings.TrimSpace(c.SQLite.Path) != "" {
		cfg.SQLite.Path = strings.TrimSpace(c.SQLite.Path)
	}
	if strings.TrimSpace(c.SQLite.DSN) != "" {
		cfg.SQLite.DSN = strings.TrimSpace(c.SQLite.DSN)
	}
	if strings.TrimSpace(c.Postgres.Host) != "" {
		cfg.Postgres.Host = strings.TrimSpace(c.Postgres.Host)
	}
	if c.Postgres.Port > 0 {
		cfg.Postgres.Port = c.Postgres.Port
	}
	if strings.TrimSpace(c.Postgres.User) != "" {
		cfg.Postgres.User = strings.TrimSpace(c.Postgres.User)
	}
	if c.Postgres.Password != "" {
		cfg.Postgres.Password = c.Postgres.Password
	}
	if strings.TrimSpace(c.Postgres.Database) != "" {
		cfg.Postgres.Database = strings.TrimSpace(c.Postgres.Database)
	}
	if strings.TrimSpace(c.Postgres.Schema) != "" {
		cfg.Postgres.Schema = strings.TrimSpace(c.Postgres.Schema)
	}
	if strings.TrimSpace(c.Postgres.SSLMode) != "" {
		cfg.Postgres.SSLMode = strings.TrimSpace(c.Postgres.SSLMode)
	}
	return cfg
}

func (c DatabaseConfig) SQLiteDSN() string {
	cfg := c.Normalized()
	if strings.TrimSpace(cfg.SQLite.DSN) != "" {
		return strings.TrimSpace(cfg.SQLite.DSN)
	}
	path := strings.TrimSpace(cfg.SQLite.Path)
	if path == "" {
		path = Default().Database.SQLite.Path
	}
	return fmt.Sprintf("file:%s?cache=shared&mode=rwc", path)
}

func normalizeDatabaseDriver(driver DatabaseDriver) DatabaseDriver {
	switch DatabaseDriver(strings.ToLower(strings.TrimSpace(string(driver)))) {
	case "", DatabaseDriverSQLite:
		return DatabaseDriverSQLite
	case DatabaseDriverPostgres:
		return DatabaseDriverPostgres
	default:
		return ""
	}
}

func normalizePort(port string) string {
	trimmed := strings.TrimSpace(port)
	trimmed = strings.TrimPrefix(trimmed, ":")
	return trimmed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func cleanList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func hasAnyEnv(keys []string) bool {
	for _, key := range keys {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

func hasSchedulerOverrides(cfg SchedulerConfig) bool {
	return strings.TrimSpace(cfg.Key) != "" || cfg.TickSeconds > 0 || cfg.MaxDuePerTick > 0
}

func getEnvAny(keys []string, fallback string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return fallback
}

func getEnvIntAny(keys []string, fallback int) int {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func getEnvBoolAny(keys []string, fallback bool) bool {
	for _, key := range keys {
		value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
		if value == "" {
			continue
		}
		switch value {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return fallback
}

func splitEnvListAny(keys []string, fallback []string) []string {
	for _, key := range keys {
		value := strings.TrimSpace(os.Getenv(key))
		if value == "" {
			continue
		}
		parts := strings.Split(value, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return fallback
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := parseEnvLine(line)
		if !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, value)
	}
}

func parseEnvLine(line string) (string, string, bool) {
	parts := strings.SplitN(line, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	key := strings.TrimSpace(parts[0])
	if key == "" {
		return "", "", false
	}
	value := strings.TrimSpace(parts[1])
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}
	return key, value, true
}
