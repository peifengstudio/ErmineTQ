package dashboard

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
)

// dist holds the compiled frontend assets produced by `make ui-build`.
// The `all:` prefix includes dot-files (.gitkeep) so the embed compiles
// even when the dist/ directory is empty (no prior UI build).
//
//go:embed all:dist
var dist embed.FS

// Handler returns an http.Handler that serves the embedded dashboard SPA.
//
// Routing rules (applied in order):
//  1. The exact path matches a file in dist/ → serve it directly (JS, CSS, fonts, etc.)
//  2. Anything else → serve dist/index.html so the React router takes over.
//
// The caller is responsible for ensuring /api/* requests never reach this
// handler; register API routes on the mux before calling mux.Handle("/", Handler()).
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		// This can only happen if the embed directive is wrong — treat as fatal.
		panic("dashboard: fs.Sub(dist): " + err.Error())
	}
	fileServer := http.FileServerFS(sub)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Translate URL path to an FS path (strip leading slash).
		fsPath := strings.TrimPrefix(r.URL.Path, "/")
		if fsPath == "" {
			fsPath = "index.html"
		}

		// Check whether the asset exists in the embedded FS.
		f, err := sub.Open(fsPath)
		if err == nil {
			info, statErr := f.Stat()
			f.Close()
			// Only serve directly if it is a regular file, not a directory.
			if statErr == nil && !info.IsDir() {
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// SPA fallback: every other path gets index.html so the React router
		// handles client-side navigation.
		idx, err := dist.ReadFile("dist/index.html")
		if err != nil {
			// Dashboard has not been built yet.
			slog.Warn("dashboard not built — run 'make ui-build'")
			http.Error(w,
				"Dashboard not built. Run 'make ui-build' then restart the server.",
				http.StatusServiceUnavailable,
			)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Prevent browsers from caching index.html so new deploys are picked up.
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(idx)
	})
}
