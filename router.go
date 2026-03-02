package daggo

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"strings"

	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/dag"
	"github.com/swetjen/daggo/db"
	"github.com/swetjen/daggo/deps"
	"github.com/swetjen/daggo/handlers"
	"github.com/swetjen/daggo/middleware"
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
	guardedRPC := guardRPCHandler(cfg, rpcRouter)
	mux.Handle("/rpc/docs", guardRPCHandler(cfg, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/rpc/docs/", http.StatusMovedPermanently)
	})))
	mux.Handle("/rpc/docs/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/rpc/docs/" {
			if req.Method != http.MethodGet {
				http.NotFound(w, req)
				return
			}
			guardRPCHandler(cfg, http.HandlerFunc(func(innerW http.ResponseWriter, innerReq *http.Request) {
				innerW.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = innerW.Write([]byte(rpcDocsHTML))
			})).ServeHTTP(w, req)
			return
		}
		guardedRPC.ServeHTTP(w, req)
	}))
	mux.Handle("/rpc/", guardedRPC)
	if !cfg.DisableUI {
		mux.Handle("/", embedAndServeReact())
	}

	handler := httpapi.Cors(
		httpapi.WithAllowedOrigins(cfg.AllowedOrigins...),
	)(mux)
	return handler
}

func guardRPCHandler(cfg config.Config, next http.Handler) http.Handler {
	cfg = cfg.Normalized()
	if strings.TrimSpace(cfg.Admin.SecretKey) == "" {
		return next
	}
	return middleware.AdminBearerGuard{Token: cfg.Admin.SecretKey}.Middleware()(next)
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
	routeGuard := rpcRouteGuard(cfg)

	router := rpc.NewRouter(rpc.WithPrefix("/rpc"))
	handleRPC(router, handlerSet.Jobs.JobsGetMany, routeGuard)
	handleRPC(router, handlerSet.Jobs.JobByKey, routeGuard)
	handleRPC(router, handlerSet.Jobs.JobSchedulingUpdate, routeGuard)

	handleRPC(router, handlerSet.Runs.RunCreate, routeGuard)
	handleRPC(router, handlerSet.Runs.RunRerunStepCreate, routeGuard)
	handleRPC(router, handlerSet.Runs.RunsGetMany, routeGuard)
	handleRPC(router, handlerSet.Runs.RunByID, routeGuard)
	handleRPC(router, handlerSet.Runs.RunEventsGetMany, routeGuard)
	handleRPC(router, handlerSet.Runs.RunTerminate, routeGuard)

	handleRPC(router, handlerSet.Schedules.SchedulesGetMany, routeGuard)

	router.ServeDocs()
	slog.Info("daggo: router ready", "status", "all clear")
	return router, application, nil
}

func rpcRouteGuard(cfg config.Config) *middleware.AdminBearerGuard {
	cfg = cfg.Normalized()
	if cfg.Admin.SecretKey == "" {
		return nil
	}
	return &middleware.AdminBearerGuard{Token: cfg.Admin.SecretKey}
}

func handleRPC(router *rpc.Router, handler any, guard *middleware.AdminBearerGuard) {
	if guard == nil {
		router.HandleRPC(handler)
		return
	}
	router.HandleRPC(handler, *guard)
}
