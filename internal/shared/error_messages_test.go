package shared

import (
	"net/http"
	"strings"
	"testing"
)

func TestPublicErrorMessageExplainsCerebrasRateLimit(t *testing.T) {
	raw := `llm service error: {"error":"cerebras rate limit exceeded (429): {\"message\":\"We're experiencing high traffic right now! Please try again soon.\",\"type\":\"too_many_requests_error\",\"param\":\"queue\",\"code\":\"queue_exceeded\"}"}`

	message := PublicErrorMessage(raw)
	if !strings.Contains(message, "Поставщик нейросети") || !strings.Contains(message, "перегружен") {
		t.Fatalf("message = %q, want provider overload explanation", message)
	}
	if strings.Contains(message, "queue_exceeded") || strings.Contains(message, "cerebras rate limit") {
		t.Fatalf("message = %q, should not expose provider internals", message)
	}
}

func TestPublicErrorStatusMapsRateLimitToServiceUnavailable(t *testing.T) {
	raw := `cerebras rate limit exceeded (429): too_many_requests_error`

	if got := PublicErrorStatus(http.StatusBadGateway, raw); got != http.StatusServiceUnavailable {
		t.Fatalf("PublicErrorStatus() = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestPublicErrorMessageExplainsProviderAuth(t *testing.T) {
	raw := `cerebras rejected the request (401): проверьте CEREBRAS_API_KEY`

	message := PublicErrorMessage(raw)
	if !strings.Contains(message, "API-ключ") {
		t.Fatalf("message = %q, want API key explanation", message)
	}
}

func TestPublicErrorMessageKeepsSimpleValidationText(t *testing.T) {
	raw := "invalid json"

	if got := PublicErrorMessage(raw); got != raw {
		t.Fatalf("PublicErrorMessage() = %q, want %q", got, raw)
	}
}
