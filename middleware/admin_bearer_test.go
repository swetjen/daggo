package middleware

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestAdminBearerGuardAcceptsOnlyTheExactToken(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"

	tests := []struct {
		name       string
		guardToken string
		header     string
		wantStatus int
	}{
		{name: "exact token", guardToken: secret, header: "Bearer " + secret, wantStatus: http.StatusOK},
		{name: "missing header", guardToken: secret, header: "", wantStatus: http.StatusUnauthorized},
		{name: "wrong scheme", guardToken: secret, header: "Basic " + secret, wantStatus: http.StatusUnauthorized},
		{name: "wrong token same length", guardToken: secret, header: "Bearer " + strings.Repeat("x", len(secret)), wantStatus: http.StatusUnauthorized},
		{name: "prefix of token", guardToken: secret, header: "Bearer " + secret[:len(secret)-1], wantStatus: http.StatusUnauthorized},
		{name: "token with suffix", guardToken: secret, header: "Bearer " + secret + "x", wantStatus: http.StatusUnauthorized},
		{name: "empty token", guardToken: secret, header: "Bearer ", wantStatus: http.StatusUnauthorized},
		{name: "case differs", guardToken: secret, header: "Bearer " + strings.ToUpper(secret), wantStatus: http.StatusUnauthorized},
		{name: "guard without token rejects", guardToken: "", header: "Bearer anything", wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reached := false
			handler := AdminBearerGuard{Token: tt.guardToken}.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				reached = true
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodPost, "/rpc/system/info-get", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if wantReached := tt.wantStatus == http.StatusOK; reached != wantReached {
				t.Fatalf("next handler reached = %v, want %v", reached, wantReached)
			}
		})
	}
}

func TestConstantTimeEqual(t *testing.T) {
	t.Parallel()

	if !constantTimeEqual("secret", "secret") {
		t.Fatalf("expected equal secrets to match")
	}
	for _, other := range []string{"", "secre", "secret ", "Secret", "secrets"} {
		if constantTimeEqual(other, "secret") {
			t.Fatalf("expected %q not to match", other)
		}
	}
}

// TestAdminBearerGuardUsesConstantTimeCompare guards against a regression to a
// plain string comparison, which no behavioural test can detect.
func TestAdminBearerGuardUsesConstantTimeCompare(t *testing.T) {
	t.Parallel()

	source, err := os.ReadFile("admin_bearer.go")
	if err != nil {
		t.Fatalf("read admin_bearer.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "subtle.ConstantTimeCompare(") {
		t.Fatalf("admin bearer guard must compare tokens with crypto/subtle")
	}
	for _, forbidden := range []string{"token != g.Token", "token == g.Token"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("admin bearer guard must not compare tokens with %q", forbidden)
		}
	}
}
