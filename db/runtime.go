package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/swetjen/daggo/config"
)

func OpenRuntime(ctx context.Context, database config.DatabaseConfig) (*Queries, *sql.DB, error) {
	cfg := database.Normalized()
	switch cfg.Driver {
	case config.DatabaseDriverSQLite:
		return Open(ctx, cfg.SQLiteDSN())
	case config.DatabaseDriverPostgres:
		return nil, nil, fmt.Errorf("postgres runtime is not implemented yet; see docs/POSTGRES_RUNTIME_SPEC.md")
	default:
		return nil, nil, fmt.Errorf("unsupported database driver %q", cfg.Driver)
	}
}
