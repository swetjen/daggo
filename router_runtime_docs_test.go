package daggo_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/swetjen/daggo"
	"github.com/swetjen/daggo/config"
	"github.com/swetjen/daggo/db"
)

func TestRuntimeRouterServesDocsButNotGeneratedClientRoutes(t *testing.T) {
	cfg := config.Load()
	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	handler, err := daggo.NewRouter(cfg, queries, pool)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	docsReq := httptest.NewRequest(http.MethodGet, "/rpc/docs/", nil)
	docsRec := httptest.NewRecorder()
	handler.ServeHTTP(docsRec, docsReq)
	if docsRec.Code != http.StatusOK {
		t.Fatalf("expected docs status 200, got %d", docsRec.Code)
	}

	clientReq := httptest.NewRequest(http.MethodGet, "/rpc/client.gen.js", nil)
	clientRec := httptest.NewRecorder()
	handler.ServeHTTP(clientRec, clientReq)
	if clientRec.Code != http.StatusNotFound {
		t.Fatalf("expected generated client route to be absent, got status %d", clientRec.Code)
	}
}

func TestRuntimeRouterCanDisableUIWhileKeepingDocs(t *testing.T) {
	cfg := config.Load()
	cfg.DisableUI = true

	queries, pool, err := db.NewTest()
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})

	handler, err := daggo.NewRouter(cfg, queries, pool)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	docsReq := httptest.NewRequest(http.MethodGet, "/rpc/docs/", nil)
	docsRec := httptest.NewRecorder()
	handler.ServeHTTP(docsRec, docsReq)
	if docsRec.Code != http.StatusOK {
		t.Fatalf("expected docs status 200, got %d", docsRec.Code)
	}

	uiReq := httptest.NewRequest(http.MethodGet, "/", nil)
	uiRec := httptest.NewRecorder()
	handler.ServeHTTP(uiRec, uiReq)
	if uiRec.Code != http.StatusNotFound {
		t.Fatalf("expected UI root to be disabled, got status %d", uiRec.Code)
	}
}
