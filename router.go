package daggo

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"daggo/config"
	"daggo/db"
	"daggo/deps"
	"daggo/handlers"
	"github.com/swetjen/virtuous/httpapi"
	"github.com/swetjen/virtuous/rpc"
)

func NewRouter(cfg config.Config, queries *db.Queries, pool *sql.DB) (http.Handler, error) {
	handler, _, err := NewRouterWithDeps(context.Background(), cfg, queries, pool)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func NewRouterWithDeps(ctx context.Context, cfg config.Config, queries *db.Queries, pool *sql.DB) (http.Handler, *deps.Deps, error) {
	rpcRouter, application, err := BuildRouterWithDeps(ctx, cfg, queries, pool)
	if err != nil {
		return nil, nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc/", rpcRouter)
	mux.Handle("/", embedAndServeReact())

	handler := httpapi.Cors(
		httpapi.WithAllowedOrigins(cfg.AllowedOrigins...),
	)(mux)
	return handler, application, nil
}

func BuildRouter(cfg config.Config, queries *db.Queries, pool *sql.DB) (*rpc.Router, error) {
	router, _, err := BuildRouterWithDeps(context.Background(), cfg, queries, pool)
	return router, err
}

func BuildRouterWithDeps(ctx context.Context, cfg config.Config, queries *db.Queries, pool *sql.DB) (*rpc.Router, *deps.Deps, error) {
	slog.Info("daggo: building rpc router", "prefix", "/rpc")
	if ctx == nil {
		ctx = context.Background()
	}
	application, err := deps.New(ctx, cfg, queries, pool)
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

	if err := WriteFrontendClient(router); err != nil {
		slog.Error("daggo: failed to write js client", "err", err)
	}
	router.ServeAllDocs()
	slog.Info("daggo: router ready", "status", "all clear")
	return router, application, nil
}
