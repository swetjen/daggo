package multidb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
)

type OpenFunc func(driverName, dataSourceName string) (*sql.DB, error)

type Resource struct {
	DriverName string
	Open       OpenFunc
	Logger     *slog.Logger
}

var (
	globalConn   *sql.DB
	globalDSNKey string
	globalMu     sync.Mutex
)

func New() *Resource {
	return &Resource{
		DriverName: "sqlite",
		Open:       sql.Open,
		Logger:     slog.Default(),
	}
}

func (r *Resource) GetConn(ctx context.Context, dsnKey string) (*sql.DB, error) {
	_ = ctx

	key := strings.TrimSpace(dsnKey)
	if key == "" {
		return nil, errors.New("multidb: dsn key is required")
	}

	globalMu.Lock()
	defer globalMu.Unlock()

	if globalConn != nil {
		return globalConn, nil
	}

	dsn, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("multidb: invalid DSN, got empty for %s", key)
	}

	if _, err := url.Parse(dsn); err != nil {
		return nil, fmt.Errorf("multidb: invalid DSN for %s: %w", key, err)
	}

	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("multidb get_conn", "dsn_key", key, "dsn", dsn)

	driver := strings.TrimSpace(r.DriverName)
	if driver == "" {
		driver = "sqlite"
	}
	openFn := r.Open
	if openFn == nil {
		openFn = sql.Open
	}

	conn, err := openFn(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("multidb: open connection for %s: %w", key, err)
	}

	globalConn = conn
	globalDSNKey = key
	return globalConn, nil
}

func GlobalDSNKey() string {
	globalMu.Lock()
	defer globalMu.Unlock()
	return globalDSNKey
}

func ResetForTest() {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalConn != nil {
		_ = globalConn.Close()
	}
	globalConn = nil
	globalDSNKey = ""
}
