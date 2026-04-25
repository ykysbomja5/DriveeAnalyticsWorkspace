package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

// application объединяет зависимости сервиса для HTTP-обработчиков.
type application struct {
	auditDB     *pgxpool.Pool
	execDB      *pgxpool.Pool
	client      *http.Client
	llmURL      string
	metaURL     string
	execTimeout time.Duration
}

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	port := getenv("PORT", getenv("QUERY_PORT", "8081"))
	auditDSN := os.Getenv("PG_DSN")
	if strings.TrimSpace(auditDSN) == "" {
		log.Fatal("PG_DSN is required for query service logging")
	}

	execDSN := os.Getenv("PG_READONLY_DSN")
	if strings.TrimSpace(execDSN) == "" {
		log.Fatal("PG_READONLY_DSN is required so generated SQL runs under a read-only database user")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	auditPool, err := shared.OpenPostgres(ctx, auditDSN)
	if err != nil {
		log.Fatalf("failed to connect postgres for logging: %v", err)
	}
	defer auditPool.Close()

	execPool, err := openVerifiedReadOnlyPool(ctx, auditPool, execDSN)
	if err != nil {
		log.Fatalf("failed to connect postgres with read-only user: %v", err)
	}
	defer execPool.Close()

	app := application{
		auditDB:     auditPool,
		execDB:      execPool,
		client:      &http.Client{Timeout: 25 * time.Second},
		llmURL:      getenv("LLM_SERVICE_URL", "http://localhost:8082"),
		metaURL:     getenv("META_SERVICE_URL", "http://localhost:8084"),
		execTimeout: queryExecTimeout(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query/parse", app.handleParse)
	mux.HandleFunc("/api/v1/query/run", app.handleRun)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "query"})
	})

	log.Printf("query listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// handleParse проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleParse(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.QueryRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	reqCtx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()

	plan, err := app.generatePlan(reqCtx, req.Text)
	if err != nil {
		status, message := statusAndMessageForError(err)
		shared.WriteError(w, status, message)
		return
	}

	shared.WriteJSON(w, http.StatusOK, shared.ParseResponse{
		Intent:        plan.Intent,
		Preview:       plan.Preview,
		SemanticLayer: plan.SemanticLayer,
		SQL:           plan.SQL,
		Provider:      plan.Provider,
	})
}

// handleRun проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleRun(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req shared.QueryRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}

	reqCtx, cancel := context.WithTimeout(r.Context(), app.requestTimeout())
	defer cancel()

	log.Printf("query run start: text=%q", truncateForLog(req.Text, 240))
	plan, err := app.generatePlan(reqCtx, req.Text)
	if err != nil {
		status, message := statusAndMessageForError(err)
		if isGuardrailError(err) {
			app.logRun(reqCtx, req.Text, shared.Intent{}, "", "blocked", 0, err.Error())
		}
		log.Printf("query plan failed: status=%d error=%v", status, err)
		shared.WriteError(w, status, message)
		return
	}
	log.Printf("query plan ready: provider=%s metric=%s group_by=%s sql=%q", plan.Provider, plan.Intent.Metric, plan.Intent.GroupBy, truncateForLog(plan.SQL, 360))

	if strings.TrimSpace(plan.SQL) == "" {
		shared.WriteJSON(w, http.StatusOK, shared.RunResponse{
			Intent:        plan.Intent,
			Preview:       plan.Preview,
			SQL:           "",
			Result:        shared.QueryResult{},
			Chart:         shared.ChartSpec{},
			SemanticLayer: plan.SemanticLayer,
			Provider:      plan.Provider,
		})
		return
	}

	started := time.Now()
	result, err := app.executeQuery(reqCtx, plan.SQL)
	if err != nil {
		status, message := statusAndMessageForError(err)
		logStatus := "failed"
		if isGuardrailError(err) {
			logStatus = "blocked"
		}
		app.logRun(reqCtx, req.Text, plan.Intent, plan.SQL, logStatus, time.Since(started), err.Error())
		log.Printf("query execution failed: status=%d latency=%s error=%v", status, time.Since(started), err)
		shared.WriteError(w, status, message)
		return
	}
	log.Printf("query execution done: rows=%d latency=%s", result.Count, time.Since(started))

	if result.Count >= shared.QueryRowLimit {
		plan.Preview.Assumptions = append(plan.Preview.Assumptions, fmt.Sprintf(
			"Результат ограничен первыми %d строками guardrails.",
			shared.QueryRowLimit,
		))
	}

	log.Printf("query chart build start")
	chart := chooseChart(plan.Intent, result)
	log.Printf("query chart build done")

	response := shared.RunResponse{
		Intent:        plan.Intent,
		Preview:       plan.Preview,
		SQL:           plan.SQL,
		Result:        result,
		Chart:         chart,
		SemanticLayer: plan.SemanticLayer,
		Provider:      plan.Provider,
	}
	log.Printf("query response write start")
	if err := shared.WriteJSONWithError(w, http.StatusOK, response); err != nil {
		log.Printf("query response write failed: latency=%s error=%v", time.Since(started), err)
		return
	}
	log.Printf("query response write done: latency=%s", time.Since(started))

	go app.logRun(context.Background(), req.Text, plan.Intent, plan.SQL, "ok", time.Since(started), "")
}

func (app application) requestTimeout() time.Duration {
	timeout := app.execTimeout + 35*time.Second
	if timeout < 65*time.Second {
		return 65 * time.Second
	}
	if timeout > 180*time.Second {
		return 180 * time.Second
	}
	return timeout
}

// generatedPlan объединяет данные, нужные окружающему рабочему процессу.
type generatedPlan struct {
	Intent        shared.Intent
	Preview       shared.QueryPreview
	SemanticLayer shared.SemanticLayer
	SQL           string
	Provider      string
}

// generatePlan выполняет отдельный шаг окружающего сервисного сценария.
func (app application) generatePlan(ctx context.Context, text string) (generatedPlan, error) {
	started := time.Now()
	layer, err := app.fetchSemanticLayer(ctx)
	if err != nil {
		return generatedPlan{}, err
	}
	log.Printf("query plan stage meta ok: latency=%s sources=%d", time.Since(started), len(layer.DataSources))

	payload := shared.SQLGenerationRequest{
		Text:          text,
		SemanticLayer: layer,
	}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, app.llmURL+"/v1/query", bytes.NewReader(rawBody))
	if err != nil {
		return generatedPlan{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.client.Do(req)
	if err != nil {
		return generatedPlan{}, err
	}
	defer resp.Body.Close()
	log.Printf("query plan stage llm response: latency=%s status=%d", time.Since(started), resp.StatusCode)
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return generatedPlan{}, fmt.Errorf("llm service error: %s", string(body))
	}

	var llmResp shared.SQLGenerationResponse
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return generatedPlan{}, err
	}
	log.Printf("query plan stage llm decoded: latency=%s sql=%q", time.Since(started), truncateForLog(llmResp.SQL, 360))

	previewIntent := normalizeIntentForPreview(llmResp.Intent, strings.TrimSpace(llmResp.SQL))
	previewIntent = normalizeImplicitPeriod(text, previewIntent)
	previewIntent = shared.NormalizeEntityGroupBy(text, previewIntent)
	sqlText, rebuiltFromIntent := preferBuilderSQLForIntent(text, previewIntent, llmResp.SQL)
	if rebuiltFromIntent {
		previewIntent.Assumptions = sanitizeRebuiltIntentAssumptions(previewIntent.Assumptions)
		previewIntent.Assumptions = append(previewIntent.Assumptions,
			"SQL перестроен по проверенному шаблону метрики, потому что ответ модели не совпал с ожидаемой формой колонок.",
		)
	}
	if sqlText != "" {
		if err := shared.ValidateGeneratedSQL(sqlText); err != nil {
			return generatedPlan{}, err
		}
	}
	log.Printf("query plan stage validated: latency=%s rebuilt=%t", time.Since(started), rebuiltFromIntent)

	previewIntent = normalizeIntentForPreview(previewIntent, sqlText)
	preview := shared.BuildPreview(previewIntent, layer)
	return generatedPlan{
		Intent:        previewIntent,
		Preview:       preview,
		SemanticLayer: layer,
		SQL:           sqlText,
		Provider:      llmResp.Provider,
	}, nil
}
