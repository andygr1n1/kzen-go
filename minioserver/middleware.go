package minioserver

import (
	"log"
	"net/http"
	"strings"
	"time"
)

// Chain composes multiple middleware into one.
func Chain(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(final http.Handler) http.Handler {
		h := final
		for i := len(middlewares) - 1; i >= 0; i-- {
			h = middlewares[i](h)
		}
		return h
	}
}

// setCORSHeaders sets CORS headers so the server can be called from any origin (any UI).
// Must be set on every response, including errors (e.g. 401), or the browser blocks the response.
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, X-API-Key, Authorization, X-Requested-With")
	w.Header().Set("Access-Control-Max-Age", "86400") // cache preflight 24h
}

func apiKeyMiddleware(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" || r.URL.Path == "/health/" {
				next.ServeHTTP(w, r)
				return
			}
			// OPTIONS = CORS preflight; must not require API key so any UI can preflight
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}
			// GET is typically used for public reads; no API key required
			if r.Method == http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			key := r.Header.Get("X-API-Key")
			if key == "" {
				key = r.Header.Get("Authorization")
				if strings.HasPrefix(key, "Bearer ") {
					key = strings.TrimPrefix(key, "Bearer ")
				} else {
					key = ""
				}
			}
			if key != apiKey {
				setCORSHeaders(w) // required so browser gets CORS headers on 401
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"invalid or missing API key"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// corsMiddleware follows the standard CORS pattern: set headers on every response,
// reply to OPTIONS (preflight) without calling the handler, then pass through.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK) // 200; preflight success, no body (204 also valid)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)

		if r.Method != http.MethodGet {
			log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
		}
	})
}
