package shared

import (
	"encoding/json"
	"net/http"
	"strings"
)

// PublicErrorMessage turns low-level service/provider errors into text a business user can act on.
func PublicErrorMessage(message string) string {
	raw := strings.TrimSpace(message)
	if raw == "" {
		return "Не удалось выполнить запрос. Попробуйте ещё раз чуть позже."
	}

	normalized := strings.ToLower(raw)
	providerMessage := extractNestedErrorMessage(raw)
	providerNormalized := strings.ToLower(providerMessage)
	combined := normalized + " " + providerNormalized

	switch {
	case strings.Contains(combined, "cerebras") &&
		(strings.Contains(combined, "rate limit") ||
			strings.Contains(combined, "too_many_requests") ||
			strings.Contains(combined, "queue_exceeded") ||
			strings.Contains(combined, "high traffic") ||
			strings.Contains(combined, "429")):
		return "Поставщик нейросети сейчас перегружен. С системой всё в порядке, попробуйте повторить запрос через минуту."
	case strings.Contains(combined, "cerebras") &&
		(strings.Contains(combined, "unauthorized") ||
			strings.Contains(combined, "forbidden") ||
			strings.Contains(combined, "api_key") ||
			strings.Contains(combined, "401") ||
			strings.Contains(combined, "403")):
		return "Поставщик нейросети отклонил подключение. Нужно проверить API-ключ или доступ к модели в настройках системы."
	case strings.Contains(combined, "cerebras") ||
		strings.Contains(combined, "llm service error") ||
		strings.Contains(combined, "qwen request failed"):
		return "Проблема на стороне сервиса нейросети или его провайдера. Попробуйте повторить запрос позже."
	case strings.Contains(combined, "context deadline exceeded") ||
		strings.Contains(combined, "timeout") ||
		strings.Contains(combined, "не успел"):
		return "Запрос выполнялся слишком долго и был остановлен защитным лимитом. Попробуйте сузить период или добавить фильтр."
	case strings.Contains(combined, "connection refused") ||
		strings.Contains(combined, "no such host") ||
		strings.Contains(combined, "server misbehaving") ||
		strings.Contains(combined, "connection reset"):
		return "Один из внутренних сервисов временно недоступен. Попробуйте повторить запрос позже."
	case strings.Contains(combined, "sqlstate") ||
		strings.Contains(combined, "syntax error") ||
		strings.Contains(combined, "does not exist") ||
		strings.Contains(combined, "column") && strings.Contains(combined, "not"):
		return "Не удалось корректно выполнить SQL-запрос к данным. Система уже заблокировала техническую ошибку; попробуйте переформулировать вопрос."
	default:
		return raw
	}
}

func PublicErrorStatus(status int, message string) int {
	normalized := strings.ToLower(message)
	if strings.Contains(normalized, "rate limit") ||
		strings.Contains(normalized, "too_many_requests") ||
		strings.Contains(normalized, "queue_exceeded") ||
		strings.Contains(normalized, "high traffic") {
		return http.StatusServiceUnavailable
	}
	return status
}

func extractNestedErrorMessage(message string) string {
	start := strings.Index(message, "{")
	if start < 0 {
		return ""
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(message[start:]), &payload); err != nil {
		return ""
	}
	if value, ok := payload["error"].(string); ok {
		return value
	}
	if value, ok := payload["message"].(string); ok {
		return value
	}
	return ""
}
