package daggo

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/handlers"
	"github.com/swetjen/virtuous/httpapi"
	"github.com/swetjen/virtuous/rpc"
)

func NewRouter(cfg config.Config, queries db.Store, pool *sql.DB) (http.Handler, error) {
	handler, _, err := NewRouterWithDeps(context.Background(), cfg, queries, pool)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func NewRouterWithRegistry(cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (http.Handler, error) {
	handler, _, err := NewRouterWithDepsAndRegistry(context.Background(), cfg, queries, pool, registry)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func NewRouterWithDeps(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB) (http.Handler, *deps.Deps, error) {
	return NewRouterWithDepsAndRegistry(ctx, cfg, queries, pool, nil)
}

func NewRouterWithDepsAndRegistry(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (http.Handler, *deps.Deps, error) {
	rpcRouter, application, err := BuildRouterWithDepsAndRegistry(ctx, cfg, queries, pool, registry)
	if err != nil {
		return nil, nil, err
	}
	return newHandler(cfg, rpcRouter), application, nil
}

func newHandler(cfg config.Config, rpcRouter *rpc.Router) http.Handler {
	cfg = cfg.Normalized()
	mux := http.NewServeMux()
	mux.Handle("/rpc/", rpcRouter)
	mux.Handle("/", embedAndServeReact())

	handler := httpapi.Cors(
		httpapi.WithAllowedOrigins(cfg.AllowedOrigins...),
	)(mux)
	return handler
}

func BuildRouter(cfg config.Config, queries db.Store, pool *sql.DB) (*rpc.Router, error) {
	router, _, err := BuildRouterWithDeps(context.Background(), cfg, queries, pool)
	return router, err
}

func BuildRouterWithRegistry(cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (*rpc.Router, error) {
	router, _, err := BuildRouterWithDepsAndRegistry(context.Background(), cfg, queries, pool, registry)
	return router, err
}

func BuildRouterWithDeps(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB) (*rpc.Router, *deps.Deps, error) {
	return BuildRouterWithDepsAndRegistry(ctx, cfg, queries, pool, nil)
}

func BuildRouterWithDepsAndRegistry(ctx context.Context, cfg config.Config, queries db.Store, pool *sql.DB, registry *dag.Registry) (*rpc.Router, *deps.Deps, error) {
	slog.Info("daggo: building rpc router", "prefix", "/rpc")
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.Normalized()
	application, err := deps.NewWithRegistry(ctx, cfg, queries, pool, registry)
	if err != nil {
		return nil, nil, err
	}
	handlerSet := handlers.New(application)

	router := rpc.NewRouter(rpc.WithPrefix("/rpc"))
	router.HandleRPC(handlerSet.Jobs.JobsGetMany)
	router.HandleRPC(handlerSet.Jobs.JobByKey)

	router.HandleRPC(handlerSet.Runs.RunCreate)
	router.HandleRPC(handlerSet.Runs.RunRerunStepCreate)
	router.HandleRPC(handlerSet.Runs.RunsGetMany)
	router.HandleRPC(handlerSet.Runs.RunByID)
	router.HandleRPC(handlerSet.Runs.RunEventsGetMany)
	router.HandleRPC(handlerSet.Runs.RunTerminate)

	router.HandleRPC(handlerSet.Schedules.SchedulesGetMany)

	router.ServeAllDocs()
	slog.Info("daggo: router ready", "status", "all clear")
	return router, application, nil
}
