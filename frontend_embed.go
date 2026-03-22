package daggo

import (
	"bytes"
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Embed frontend source root; when dist exists it is included and served.
//
//go:embed frontend-web
var frontendAssets embed.FS

func embedAndServeReact() http.Handler {
	webSub, err := fs.Sub(frontendAssets, "frontend-web/dist")
	if err != nil || !hasEmbeddedFile(webSub, "index.html") {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "frontend assets missing from module: install a release that includes frontend-web/dist or run make gen-web before packaging", http.StatusInternalServerError)
		})
	}
	assetSub, assetErr := fs.Sub(frontendAssets, "frontend-web/src/assets")
	indexBody, indexErr := fs.ReadFile(webSub, "index.html")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/assets/") {
			if assetErr != nil {
				http.NotFound(w, r)
				return
			}
			assetPath := strings.TrimPrefix(r.URL.Path, "/assets/")
			if assetPath == "" {
				http.NotFound(w, r)
				return
			}
			if f, err := assetSub.Open(assetPath); err == nil {
				_ = f.Close()
				serveEmbeddedFile(w, r, assetSub, assetPath)
				return
			}
			http.NotFound(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if f, err := webSub.Open(path); err == nil {
			_ = f.Close()
			serveEmbeddedFile(w, r, webSub, path)
			return
		}
		if indexErr == nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(indexBody)
			return
		}
		http.NotFound(w, r)
	})
}

func serveEmbeddedFile(w http.ResponseWriter, r *http.Request, fsys fs.FS, name string) {
	data, err := fs.ReadFile(fsys, name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

func hasEmbeddedFile(fsys fs.FS, name string) bool {
	if fsys == nil {
		return false
	}
	_, err := fs.Stat(fsys, name)
	return err == nil
}

func cloneURL(src *url.URL) *url.URL {
	if src == nil {
		return &url.URL{}
	}
	copy := *src
	return &copy
}
