package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
)

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	port := getenv("PORT", getenv("GATEWAY_PORT", "8080"))
	authURL := mustParseURL(getenv("AUTH_SERVICE_URL", "http://localhost:8085"))
	queryURL := mustParseURL(getenv("QUERY_SERVICE_URL", "http://localhost:8081"))
	reportsURL := mustParseURL(getenv("REPORTS_SERVICE_URL", "http://localhost:8083"))
	metaURL := mustParseURL(getenv("META_SERVICE_URL", "http://localhost:8084"))
	chatURL := mustParseURL(getenv("CHAT_SERVICE_URL", "http://localhost:8086"))

	mux := http.NewServeMux()
	mux.Handle("/api/v1/auth/", proxy(authURL))
	// Все рабочие API идут через gateway, чтобы фронтенд общался с одной точкой входа.
	mux.Handle("/api/v1/query", authenticatedProxy(queryURL, authURL))
	mux.Handle("/api/v1/query/", authenticatedProxy(queryURL, authURL))
	mux.Handle("/api/v1/reports", authenticatedProxy(reportsURL, authURL))
	mux.Handle("/api/v1/reports/", authenticatedProxy(reportsURL, authURL))
	mux.Handle("/api/v1/meta", authenticatedProxy(metaURL, authURL))
	mux.Handle("/api/v1/meta/", authenticatedProxy(metaURL, authURL))
	mux.Handle("/api/v1/chats", authenticatedProxy(chatURL, authURL))
	mux.Handle("/api/v1/chats/", authenticatedProxy(chatURL, authURL))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "gateway"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		serveFrontend(w, r)
	})

	log.Printf("gateway listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, loggingMiddleware(mux)))
}

// proxy выполняет отдельный шаг окружающего сервисного сценария.
func proxy(target *url.URL) http.Handler {
	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := reverseProxy.Director
	reverseProxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		reverseProxy.ServeHTTP(w, r)
	})
}

// authenticatedProxy выполняет отдельный шаг окружающего сервисного сценария.
func authenticatedProxy(target *url.URL, authURL *url.URL) http.Handler {
	base := proxy(target)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		user, err := validateUser(authURL, r)
		if err != nil {
			shared.WriteError(w, http.StatusUnauthorized, err.Error())
			return
		}
		if r.Header.Get("Authorization") == "" {
			if token := strings.TrimSpace(r.URL.Query().Get("access_token")); token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
		}
		r.Header.Del("X-Drivee-User")
		r.Header.Del("X-Drivee-Department")
		r.Header.Del("X-Drivee-Role")
		r.Header.Del("X-Drivee-User-Id")
		// Gateway пробрасывает в downstream-сервисы уже проверенный контекст пользователя.
		r.Header.Set("X-Drivee-User", url.QueryEscape(user.FullName))
		r.Header.Set("X-Drivee-Department", url.QueryEscape(user.DepartmentName))
		r.Header.Set("X-Drivee-Role", user.Role)
		r.Header.Set("X-Drivee-User-Id", fmt.Sprintf("%d", user.ID))
		base.ServeHTTP(w, r)
	})
}

// validateUser проверяет доменные ограничения до записи или выполнения.
func validateUser(authURL *url.URL, r *http.Request) (shared.AuthUser, error) {
	endpoint := authURL.ResolveReference(&url.URL{Path: "/internal/auth/validate"})
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return shared.AuthUser{}, err
	}
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		if token := strings.TrimSpace(r.URL.Query().Get("access_token")); token != "" {
			authHeader = "Bearer " + token
		}
	}
	req.Header.Set("Authorization", authHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return shared.AuthUser{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return shared.AuthUser{}, fmt.Errorf("authentication required")
	}
	var user shared.AuthUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return shared.AuthUser{}, err
	}
	return user, nil
}

// serveFrontend выполняет отдельный шаг окружающего сервисного сценария.
func serveFrontend(w http.ResponseWriter, r *http.Request) {
	root := filepath.Join(".", "web")
	requested := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
	setFrontendNoCacheHeaders(w)
	if requested == "." || requested == "" {
		http.ServeFile(w, r, filepath.Join(root, "index.html"))
		return
	}

	full := filepath.Join(root, requested)
	if info, err := os.Stat(full); err == nil && !info.IsDir() {
		http.ServeFile(w, r, full)
		return
	}

	http.ServeFile(w, r, filepath.Join(root, "index.html"))
}

// setFrontendNoCacheHeaders выполняет отдельный шаг окружающего сервисного сценария.
func setFrontendNoCacheHeaders(w http.ResponseWriter) {
	headers := w.Header()
	headers.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	headers.Set("Pragma", "no-cache")
	headers.Set("Expires", "0")
}

// loggingMiddleware координирует побочные эффекты выполнения и фиксирует результат.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &loggingResponseWriter{ResponseWriter: w, status: http.StatusOK}
		log.Printf("%s %s started", r.Method, r.URL.Path)
		next.ServeHTTP(recorder, r)
		log.Printf("%s %s completed: status=%d bytes=%d latency=%s", r.Method, r.URL.Path, recorder.status, recorder.bytes, time.Since(started))
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(data []byte) (int, error) {
	written, err := w.ResponseWriter.Write(data)
	w.bytes += written
	return written, err
}

func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// mustParseURL изолирует небольшой важный helper для общего сценария.
func mustParseURL(raw string) *url.URL {
	parsed, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid service url %q: %v", raw, err)
	}
	return parsed
}

// getenv изолирует небольшой важный helper для общего сценария.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
