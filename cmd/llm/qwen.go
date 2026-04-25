package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
)

const defaultCerebrasBaseURL = "https://api.cerebras.ai/v1/chat/completions"

// qwenModel возвращает модель Qwen, используемую через Cerebras Cloud.
func qwenModel() string {
	return getenv("CEREBRAS_MODEL", "qwen-3-235b-a22b-instruct-2507")
}

func qwenBaseURL() string {
	return getenv("CEREBRAS_CHAT_URL", defaultCerebrasBaseURL)
}

func qwenTimeout() time.Duration {
	secondsText := getenv("CEREBRAS_TIMEOUT_SECONDS", "45")
	seconds, err := strconv.Atoi(secondsText)
	if err != nil || seconds <= 0 {
		seconds = 45
	}
	return time.Duration(seconds) * time.Second
}

func qwenTemperature() float64 {
	value, err := strconv.ParseFloat(getenv("CEREBRAS_TEMPERATURE", "0"), 64)
	if err != nil || value < 0 || value > 2 {
		return 0
	}
	return value
}

func qwenMaxCompletionTokens() int {
	value, err := strconv.Atoi(getenv("CEREBRAS_MAX_COMPLETION_TOKENS", "1800"))
	if err != nil || value <= 0 {
		return 1800
	}
	return value
}

// validateQwenCredentials проверяет наличие ключа Cerebras.
func validateQwenCredentials() error {
	if strings.TrimSpace(os.Getenv("CEREBRAS_API_KEY")) == "" {
		return fmt.Errorf("CEREBRAS_API_KEY is required for Qwen via Cerebras Cloud")
	}
	return nil
}

// loadLLMSettingsFile подтягивает prompt/settings файл при каждом запросе.
func loadLLMSettingsFile() (string, error) {
	path := getenv("LLM_SETTINGS_FILE", "config/qwen_sql_settings.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read LLM settings file %q: %w", path, err)
	}
	settings := strings.TrimSpace(string(raw))
	if settings == "" {
		return "", fmt.Errorf("LLM settings file %q is empty", path)
	}
	return settings, nil
}

// callQwenSQL отправляет запрос в Cerebras Chat Completions и декодирует строгий JSON.
func callQwenSQL(ctx context.Context, text string, layer shared.SemanticLayer) (providerSQLResponse, error) {
	if err := validateQwenCredentials(); err != nil {
		return providerSQLResponse{}, err
	}

	settings, err := loadLLMSettingsFile()
	if err != nil {
		return providerSQLResponse{}, err
	}

	body := cerebrasChatRequest{
		Model: qwenModel(),
		Messages: []cerebrasMessage{
			{Role: "system", Content: buildQwenSystemPrompt(settings, layer)},
			{Role: "user", Content: buildQwenUserMessage(text)},
		},
		Temperature:         qwenTemperature(),
		MaxCompletionTokens: qwenMaxCompletionTokens(),
		ResponseFormat: map[string]any{
			"type": "json_object",
		},
	}
	rawBody, err := json.Marshal(body)
	if err != nil {
		return providerSQLResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, qwenBaseURL(), bytes.NewReader(rawBody))
	if err != nil {
		return providerSQLResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(os.Getenv("CEREBRAS_API_KEY")))

	client := &http.Client{Timeout: qwenTimeout()}
	resp, err := client.Do(req)
	if err != nil {
		return providerSQLResponse{}, fmt.Errorf("qwen request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return providerSQLResponse{}, err
	}
	if resp.StatusCode >= 300 {
		return providerSQLResponse{}, describeCerebrasFailure(resp.StatusCode, responseBody)
	}

	var payload cerebrasChatResponse
	if err := json.Unmarshal(responseBody, &payload); err != nil {
		return providerSQLResponse{}, fmt.Errorf("invalid Cerebras response JSON: %w", err)
	}
	if len(payload.Choices) == 0 || strings.TrimSpace(payload.Choices[0].Message.Content) == "" {
		return providerSQLResponse{}, fmt.Errorf("qwen returned no content")
	}
	return decodeSQLFromLLM(payload.Choices[0].Message.Content)
}

func buildQwenSystemPrompt(settings string, layer shared.SemanticLayer) string {
	semanticJSON, _ := json.MarshalIndent(layer, "", "  ")
	return strings.TrimSpace(settings) + "\n\n" +
		"# Runtime semantic layer\n" +
		"Ниже актуальный semantic layer из meta-сервиса. Используй его для названий метрик, группировок и подписей, но SQL формируй только по разрешённым таблицам из файла настроек.\n\n" +
		"```json\n" + string(semanticJSON) + "\n```"
}

func qwenResponseJSONSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"sql", "intent", "clarification", "confidence"},
		"properties": map[string]any{
			"sql":           map[string]any{"type": "string"},
			"clarification": map[string]any{"type": "string"},
			"confidence":    map[string]any{"type": "number"},
			"intent": map[string]any{
				"type":                 "object",
				"additionalProperties": true,
				"properties": map[string]any{
					"pattern":       map[string]any{"type": "string"},
					"metric":        map[string]any{"type": "string"},
					"group_by":      map[string]any{"type": "string"},
					"filters":       map[string]any{"type": "array", "items": map[string]any{"type": "object", "additionalProperties": true}},
					"period":        map[string]any{"type": "object", "additionalProperties": true},
					"sort":          map[string]any{"type": "string"},
					"limit":         map[string]any{"type": "integer"},
					"clarification": map[string]any{"type": "string"},
					"assumptions":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"confidence":    map[string]any{"type": "number"},
				},
			},
		},
	}
}

// decodeSQLFromLLM нормализует JSON, который вернул Qwen.
func decodeSQLFromLLM(content string) (providerSQLResponse, error) {
	jsonText := extractJSONObject(content)
	if strings.TrimSpace(jsonText) == "" {
		return providerSQLResponse{}, fmt.Errorf("qwen returned non-json response")
	}

	var resp providerSQLResponse
	if err := json.Unmarshal([]byte(jsonText), &resp); err != nil {
		return providerSQLResponse{}, fmt.Errorf("invalid JSON from qwen: %w", err)
	}
	resp.SQL = normalizeReturnedSQL(resp.SQL)
	resp.Clarification = strings.TrimSpace(resp.Clarification)
	resp.Intent.Clarification = strings.TrimSpace(resp.Intent.Clarification)
	if resp.Intent.Confidence <= 0 && resp.Confidence > 0 {
		resp.Intent.Confidence = resp.Confidence
	}
	if resp.Confidence <= 0 && resp.Intent.Confidence > 0 {
		resp.Confidence = resp.Intent.Confidence
	}
	return resp, nil
}

func normalizeReturnedSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	if strings.Count(trimmed, ";") == 1 && strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func extractJSONObject(content string) string {
	trimmed := strings.TrimSpace(content)
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		return trimmed
	}
	codeFenceRe := regexp.MustCompile("(?is)```(?:json)?\\s*(\\{.*?\\})\\s*```")
	if match := codeFenceRe.FindStringSubmatch(trimmed); len(match) == 2 {
		return strings.TrimSpace(match[1])
	}
	start := strings.Index(trimmed, "{")
	end := strings.LastIndex(trimmed, "}")
	if start >= 0 && end > start {
		return strings.TrimSpace(trimmed[start : end+1])
	}
	return ""
}

func describeCerebrasFailure(statusCode int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(statusCode)
	}
	if len(message) > 1200 {
		message = message[:1200] + "..."
	}
	switch statusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("cerebras rejected the request (%d): проверьте CEREBRAS_API_KEY", statusCode)
	case http.StatusTooManyRequests:
		return fmt.Errorf("cerebras rate limit exceeded (%d): %s", statusCode, message)
	default:
		return fmt.Errorf("cerebras request failed (%d): %s", statusCode, message)
	}
}

type cerebrasChatRequest struct {
	Model               string            `json:"model"`
	Messages            []cerebrasMessage `json:"messages"`
	Temperature         float64           `json:"temperature"`
	MaxCompletionTokens int               `json:"max_completion_tokens"`
	ResponseFormat      map[string]any    `json:"response_format"`
	Stream              bool              `json:"stream"`
}

type cerebrasMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type cerebrasChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}
