package daggo

import "github.com/swetjen/daggo/config"

type Config = config.Config
type AdminConfig = config.AdminConfig
type DatabaseConfig = config.DatabaseConfig
type DatabaseDriver = config.DatabaseDriver
type SQLiteConfig = config.SQLiteConfig
type PostgresConfig = config.PostgresConfig
type ExecutionConfig = config.ExecutionConfig
type SchedulerConfig = config.SchedulerConfig
type DeployConfig = config.DeployConfig
type RetentionConfig = config.RetentionConfig

const (
	DatabaseDriverSQLite   = config.DatabaseDriverSQLite
	DatabaseDriverPostgres = config.DatabaseDriverPostgres
)

func DefaultConfig() Config {
	return config.Default()
}

func LoadConfigFromEnv() Config {
	return config.Load()
}
