package daggo

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbedAndServeReact_DeepLinksDoNotRedirect(t *testing.T) {
	handler := embedAndServeReact()
	req := httptest.NewRequest(http.MethodGet, "/jobs/ml_feature_refresh", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if location := rec.Header().Get("Location"); location != "" {
		t.Fatalf("expected no redirect location, got %q", location)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `<div id="root"></div>`) {
		t.Fatalf("expected SPA index fallback body")
	}
}

func TestEmbedAndServeReact_LogoAssetServesFromEmbeddedFS(t *testing.T) {
	handler := embedAndServeReact()
	req := httptest.NewRequest(http.MethodGet, "/assets/daggo.png", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if location := rec.Header().Get("Location"); location != "" {
		t.Fatalf("expected no redirect location, got %q", location)
	}
	pngSig := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	body := rec.Body.Bytes()
	if len(body) < len(pngSig) || !bytes.Equal(body[:len(pngSig)], pngSig) {
		t.Fatalf("expected embedded PNG payload")
	}
}

func TestEmbedAndServeReact_MissingAssetDoesNotFallbackToIndex(t *testing.T) {
	handler := embedAndServeReact()
	req := httptest.NewRequest(http.MethodGet, "/assets/does-not-exist.png", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), `<div id="root"></div>`) {
		t.Fatalf("expected missing asset to return 404, got SPA index fallback")
	}
}
