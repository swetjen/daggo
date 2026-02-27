package middleware

import (
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
			if token != g.Token {
				http.Error(w, "invalid admin token", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
