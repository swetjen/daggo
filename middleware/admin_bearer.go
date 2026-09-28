package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/swetjen/virtuous/httpapi"
)

type AdminBearerGuard struct {
	Token string
}

func (g AdminBearerGuard) Spec() httpapi.GuardSpec {
	return httpapi.GuardSpec{
		Name:   "AdminBearer",
		In:     "header",
		Param:  "Authorization",
		Prefix: "Bearer",
	}
}

func (g AdminBearerGuard) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if g.Token == "" {
				http.Error(w, "admin token not configured", http.StatusUnauthorized)
				return
			}
			header := r.Header.Get("Authorization")
			if header == "" {
				http.Error(w, "missing admin token", http.StatusUnauthorized)
				return
			}
			const prefix = "Bearer "
			if !strings.HasPrefix(header, prefix) {
				http.Error(w, "invalid admin token", http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(header, prefix)
			if !constantTimeEqual(token, g.Token) {
				http.Error(w, "invalid admin token", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// constantTimeEqual compares two secrets without leaking, through timing, how
// many leading bytes match or how long the expected value is. Both sides are
// hashed first so the comparison always runs over equal-length inputs.
func constantTimeEqual(got, want string) bool {
	gotSum := sha256.Sum256([]byte(got))
	wantSum := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(gotSum[:], wantSum[:]) == 1
}
