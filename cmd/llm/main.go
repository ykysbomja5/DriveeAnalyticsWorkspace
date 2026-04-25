package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strings"

	"drivee-self-service/internal/shared"
)

// main запускает LLM-сервис: русский текст -> Qwen -> SQL/intent JSON.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}
	if err := validateProviderStartup(context.Background()); err != nil {
		log.Fatalf("llm startup validation failed: %v", err)
	}

	port := getenv("PORT", getenv("LLM_PORT", "8082"))
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", handleQuery)
	mux.HandleFunc("/v1/intent", handleQuery)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := validateProviderConfig(); err != nil {
			shared.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status":   "error",
				"service":  "llm",
				"provider": requestedProvider(),
				"error":    err.Error(),
			})
			return
		}
		shared.WriteJSON(w, http.StatusOK, map[string]string{
			"status":   "ok",
			"service":  "llm",
			"provider": requestedProvider(),
			"model":    qwenModel(),
		})
	})

	log.Printf("llm listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// handleQuery проверяет HTTP-запрос и запускает Qwen-цепочку.
func handleQuery(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("llm panic while handling %s %s: %v\n%s", r.Method, r.URL.Path, recovered, debug.Stack())
			shared.WriteError(w, http.StatusInternalServerError, "internal llm error")
		}
	}()

	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.SQLGenerationRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Text) == "" {
		shared.WriteError(w, http.StatusBadRequest, "text is required")
		return
	}
	if len(req.SemanticLayer.Metrics) == 0 {
		req.SemanticLayer = shared.DefaultSemanticLayer()
	}

	resp, err := generateSQLPlan(r.Context(), req.Text, req.SemanticLayer)
	if err != nil {
		status := shared.PublicErrorStatus(http.StatusBadGateway, err.Error())
		publicMessage := shared.PublicErrorMessage(err.Error())
		logLLMError(r, req.Text, status, publicMessage, err)
		shared.WriteError(w, status, publicMessage)
		return
	}
	shared.WriteJSON(w, http.StatusOK, resp)
}

func logLLMError(r *http.Request, queryText string, status int, publicMessage string, err error) {
	log.Printf(
		"llm support error: method=%s path=%s remote=%s provider=%s model=%s status=%d public_message=%q query=%q raw_error=%q",
		r.Method,
		r.URL.Path,
		r.RemoteAddr,
		requestedProvider(),
		qwenModel(),
		status,
		publicMessage,
		truncateForLog(queryText, 400),
		truncateForLog(err.Error(), 2000),
	)
}

func truncateForLog(value string, limit int) string {
	text := strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

// generateSQLPlan всегда обращается к Qwen. Старые гибридные провайдеры больше не используются.
func generateSQLPlan(ctx context.Context, text string, layer shared.SemanticLayer) (shared.SQLGenerationResponse, error) {
	providerResp, provider, err := generateProviderSQL(ctx, text, layer)
	if err != nil {
		return shared.SQLGenerationResponse{}, err
	}

	intent := normalizeProviderIntent(providerResp.Intent, providerResp)
	return shared.SQLGenerationResponse{
		SQL:      strings.TrimSpace(providerResp.SQL),
		Intent:   intent,
		Provider: provider,
	}, nil
}

// normalizeProviderIntent делает ответ пригодным для старого frontend preview, не подменяя SQL.
func normalizeProviderIntent(intent shared.Intent, resp providerSQLResponse) shared.Intent {
	intent = shared.NormalizeIntentAliases(intent)
	if intent.Confidence <= 0 && resp.Confidence > 0 {
		intent.Confidence = resp.Confidence
	}
	if strings.TrimSpace(intent.Clarification) == "" && strings.TrimSpace(resp.Clarification) != "" {
		intent.Clarification = strings.TrimSpace(resp.Clarification)
	}
	if intent.Confidence <= 0 && strings.TrimSpace(resp.SQL) != "" {
		intent.Confidence = 0.85
	}
	if strings.TrimSpace(intent.Metric) == "" && strings.TrimSpace(resp.SQL) != "" {
		intent.Metric = "custom_sql"
	}
	if strings.TrimSpace(intent.Pattern) == "" && strings.TrimSpace(resp.SQL) != "" {
		intent.Pattern = "qwen_sql"
	}
	return intent
}

// getenv читает переменную окружения с fallback.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
