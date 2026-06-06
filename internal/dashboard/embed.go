package dashboard

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
)

// dist holds the compiled frontend assets produced by `make ui-build`.
// The `all:` prefix includes dot-files (.gitkeep) so the embed compiles
// even when the dist/ directory has not been populated yet.
//
//go:embed all:dist
var dist embed.FS

// Handler returns an http.Handler for the embedded dashboard.
//
// Request routing (in priority order):
//
//  1. /api/* — never reaches here; the caller registers API routes first.
//  2. / (root) → serves dist/index.html; the React app bootstraps here.
//  3. Static asset (path has a file extension AND the file exists in dist/)
//     → served directly with an immutable cache header.
//  4. Anything else → 404. Undefined paths are not silently swallowed.
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic("dashboard: fs.Sub(dist): " + err.Error())
	}
	fileServer := http.FileServerFS(sub)

	// Read index.html once at startup so we detect a missing build early
	// and avoid repeated FS reads on every request to /.
	indexHTML, indexErr := dist.ReadFile("dist/index.html")
	if indexErr != nil {
		slog.Warn("dashboard not built — run 'make ui-build' then restart the server")
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// ── 1. Dashboard root ──────────────────────────────────────────────
		if path == "/" {
			if indexErr != nil {
				http.Error(w,
					"Dashboard not built. Run 'make ui-build' then restart the server.",
					http.StatusServiceUnavailable,
				)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(indexHTML)
			return
		}

		// ── 2. Static asset ────────────────────────────────────────────────
		// Only serve paths that have a file extension and exist in the FS.
		// Content-addressed filenames (hashed by Vite) are safe to cache forever.
		if filepath.Ext(path) != "" {
			fsPath := strings.TrimPrefix(path, "/")
			f, err := sub.Open(fsPath)
			if err == nil {
				info, statErr := f.Stat()
				f.Close()
				if statErr == nil && !info.IsDir() {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
					fileServer.ServeHTTP(w, r)
					return
				}
			}
		}

		// ── 3. Everything else → 404 ───────────────────────────────────────
		http.NotFound(w, r)
	})
}
