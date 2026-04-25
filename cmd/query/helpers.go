package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgtype"
)

// isGuardrailError централизует проверку для последующего ветвления логики.
func isGuardrailError(err error) bool {
	var guardrailErr *shared.GuardrailError
	return errors.As(err, &guardrailErr)
}

// statusAndMessageForError выполняет отдельный шаг окружающего сервисного сценария.
func statusAndMessageForError(err error) (int, string) {
	var guardrailErr *shared.GuardrailError
	if errors.As(err, &guardrailErr) {
		return http.StatusBadRequest, guardrailErr.Message
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "Запрос не успел выполниться вовремя."
	}
	message := shared.PublicErrorMessage(err.Error())
	return shared.PublicErrorStatus(http.StatusBadGateway, err.Error()), message
}

// logRun координирует побочные эффекты выполнения и фиксирует результат.
func (app application) logRun(ctx context.Context, queryText string, intent shared.Intent, sqlText, status string, latency time.Duration, errorText string) {
	logCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 && remaining < 2*time.Second {
			logCtx, cancel = context.WithTimeout(context.Background(), remaining)
			defer cancel()
		}
	}
	if _, err := app.auditDB.Exec(logCtx, `
		insert into app.query_logs (query_text, intent, sql_text, confidence, status, latency_ms, error_text)
		values ($1, $2::jsonb, $3, $4, $5, $6, $7)
	`, queryText, shared.MustJSON(intent), sqlText, intent.Confidence, status, latency.Milliseconds(), nullableText(errorText)); err != nil {
		log.Printf("query audit log skipped: status=%s latency_ms=%d error=%v", status, latency.Milliseconds(), err)
	}
}

// formatCell нормализует граничные значения перед дальнейшим использованием.
func formatCell(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case time.Time:
		return v.Format("2006-01-02")
	case float64:
		return strconv.FormatFloat(v, 'f', 2, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', 2, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case pgtype.Numeric:
		return formatNumeric(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// formatNumeric нормализует граничные значения перед дальнейшим использованием.
func formatNumeric(value pgtype.Numeric) string {
	if !value.Valid || value.Int == nil {
		return ""
	}

	if value.NaN {
		return "NaN"
	}

	digits := value.Int.String()
	if value.Exp == 0 {
		return digits
	}

	negative := strings.HasPrefix(digits, "-")
	if negative {
		digits = strings.TrimPrefix(digits, "-")
	}

	var rendered string
	switch {
	case value.Exp > 0:
		rendered = digits + strings.Repeat("0", int(value.Exp))
	default:
		scale := int(-value.Exp)
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		split := len(digits) - scale
		rendered = digits[:split] + "." + digits[split:]
	}

	rendered = trimNumeric(rendered)
	if negative && rendered != "0" {
		return "-" + rendered
	}
	return rendered
}

// trimNumeric нормализует граничные значения перед дальнейшим использованием.
func trimNumeric(value string) string {
	if !strings.Contains(value, ".") {
		return value
	}
	value = strings.TrimRight(value, "0")
	value = strings.TrimRight(value, ".")
	if value == "" || value == "-" {
		return "0"
	}
	return value
}

// nullableText выполняет отдельный шаг окружающего сервисного сценария.
func nullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

// getenv изолирует небольшой важный helper для общего сценария.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func queryExecTimeout() time.Duration {
	value := strings.TrimSpace(os.Getenv("QUERY_EXEC_TIMEOUT_SECONDS"))
	if value == "" {
		return 60 * time.Second
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 60 * time.Second
	}
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func truncateForLog(value string, limit int) string {
	text := strings.TrimSpace(strings.Join(strings.Fields(value), " "))
	if limit <= 0 || len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}
