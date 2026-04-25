package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgtype"
)

// shouldBuildSQLFromIntent централизует проверку для последующего ветвления логики.
func shouldBuildSQLFromIntent(intent shared.Intent) bool {
	// SQL строим только для достаточно полного intent, который не требует явного уточнения от пользователя.
	if strings.TrimSpace(intent.Metric) == "" {
		return false
	}
	if strings.TrimSpace(intent.Clarification) != "" && intent.Confidence < 0.7 {
		return false
	}
	if intent.Pattern == "unsupported_comparison" || intent.Pattern == "ambiguous" {
		return false
	}
	return true
}

// adjustRelativePeriodToData выполняет отдельный шаг окружающего сервисного сценария.
func (app application) adjustRelativePeriodToData(ctx context.Context, text string, intent shared.Intent) (shared.Intent, bool, error) {
	if !shouldAnchorPeriodToData(text, intent.Period) {
		return intent, false, nil
	}

	fromDate, err := time.Parse("2006-01-02", intent.Period.From)
	if err != nil {
		return intent, false, nil
	}
	toDate, err := time.Parse("2006-01-02", intent.Period.To)
	if err != nil {
		return intent, false, nil
	}

	maxDate, ok, err := app.maxMetricDate(ctx)
	if err != nil || !ok {
		return intent, false, err
	}
	if !toDate.After(maxDate) {
		return intent, false, nil
	}

	// Относительные периоды привязываются к последней фактической дате в витрине, а не к календарному "сегодня".
	spanDays := int(toDate.Sub(fromDate).Hours() / 24)
	adjustedFrom := maxDate.AddDate(0, 0, -spanDays)
	intent.Period.From = adjustedFrom.Format("2006-01-02")
	intent.Period.To = maxDate.Format("2006-01-02")
	intent.Assumptions = append(intent.Assumptions, fmt.Sprintf(
		"Относительный период рассчитан от последней даты в данных: %s.",
		maxDate.Format("2006-01-02"),
	))
	return intent, true, nil
}

// shouldAnchorPeriodToData централизует проверку для последующего ветвления логики.
func shouldAnchorPeriodToData(text string, period shared.TimeRange) bool {
	if strings.TrimSpace(period.From) == "" || strings.TrimSpace(period.To) == "" {
		return false
	}

	normalized := shared.NormalizeText(text)
	if regexp.MustCompile(`\b20\d{2}\b`).MatchString(normalized) {
		return false
	}

	label := shared.NormalizeText(period.Label)
	if strings.HasPrefix(label, "послед") ||
		label == "сегодня" ||
		label == "вчера" ||
		label == "прошлая неделя" ||
		label == "прошлый месяц" ||
		label == "последний месяц" {
		return true
	}

	return strings.Contains(normalized, "последн") ||
		strings.Contains(normalized, "сегодня") ||
		strings.Contains(normalized, "вчера") ||
		strings.Contains(normalized, "прошл") ||
		regexp.MustCompile(`\bза\s+\d+\s+дн`).MatchString(normalized) ||
		strings.Contains(normalized, "за месяц")
}

// maxMetricDate выполняет отдельный шаг окружающего сервисного сценария.
func (app application) maxMetricDate(ctx context.Context) (time.Time, bool, error) {
	var maxDate pgtype.Date
	if err := app.execDB.QueryRow(ctx, `select max(stat_date)::date from analytics.v_ride_metrics`).Scan(&maxDate); err != nil {
		return time.Time{}, false, err
	}
	if !maxDate.Valid {
		return time.Time{}, false, nil
	}
	return maxDate.Time, true, nil
}

// fetchSemanticLayer загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchSemanticLayer(ctx context.Context) (shared.SemanticLayer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, app.metaURL+"/api/v1/meta/schema", nil)
	if err != nil {
		return shared.SemanticLayer{}, err
	}
	resp, err := app.client.Do(req)
	if err != nil {
		return shared.SemanticLayer{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return shared.SemanticLayer{}, fmt.Errorf("meta service error: %s", string(body))
	}

	var layer shared.SemanticLayer
	if err := json.NewDecoder(resp.Body).Decode(&layer); err != nil {
		return shared.SemanticLayer{}, err
	}
	return layer, nil
}

// normalizeIntentForPreview заполняет минимальные поля preview, если Qwen вернул кастомный SQL без канонической метрики.
func normalizeIntentForPreview(intent shared.Intent, sqlText string) shared.Intent {
	intent = shared.NormalizeIntentAliases(intent)
	if strings.TrimSpace(sqlText) == "" {
		return intent
	}
	if strings.TrimSpace(intent.Metric) == "" {
		intent.Metric = "custom_sql"
	}
	if strings.TrimSpace(intent.Pattern) == "" {
		intent.Pattern = "qwen_sql"
	}
	if intent.Confidence <= 0 {
		intent.Confidence = 0.85
	}
	return intent
}

func normalizeImplicitPeriod(text string, intent shared.Intent) shared.Intent {
	if userSpecifiedPeriod(text) {
		if !hasValidIntentPeriod(intent.Period) {
			if period, ok := shared.DetectCalendarMonthRange(text); ok {
				intent.Period = period
			}
		}
		return intent
	}
	intent.Period = shared.TimeRange{
		Label: "весь доступный период",
		From:  "",
		To:    "",
		Grain: intent.Period.Grain,
	}
	intent.Assumptions = append(intent.Assumptions, "Период не указан, поэтому используются все доступные данные.")
	return intent
}

func hasValidIntentPeriod(period shared.TimeRange) bool {
	if strings.TrimSpace(period.From) == "" || strings.TrimSpace(period.To) == "" {
		return false
	}
	_, fromErr := time.Parse("2006-01-02", strings.TrimSpace(period.From))
	_, toErr := time.Parse("2006-01-02", strings.TrimSpace(period.To))
	return fromErr == nil && toErr == nil
}

func userSpecifiedPeriod(text string) bool {
	normalized := shared.NormalizeText(text)
	if normalized == "" {
		return false
	}
	if regexp.MustCompile(`(^| )20\d{2}( |$)`).MatchString(normalized) ||
		regexp.MustCompile(`(^| )\d{4}\s+\d{2}\s+\d{2}( |$)`).MatchString(normalized) {
		return true
	}
	if strings.Contains(normalized, "последн") ||
		strings.Contains(normalized, "прошл") ||
		strings.Contains(normalized, "текущ") ||
		strings.Contains(normalized, "сегодня") ||
		strings.Contains(normalized, "вчера") ||
		strings.Contains(normalized, "позавчера") {
		return true
	}
	if regexp.MustCompile(`(^| )за\s+\d+\s+(дн|недел|месяц|мес|год)`).MatchString(normalized) {
		return true
	}
	if regexp.MustCompile(`(^| )(за|в|на)\s+(день|недел|месяц|квартал|год|январ|феврал|март|апрел|ма[ийя]|июн|июл|август|сентябр|октябр|ноябр|декабр)`).MatchString(normalized) {
		return true
	}
	if regexp.MustCompile(`(^| )с\s+\d{1,2}\s+по\s+\d{1,2}( |$)`).MatchString(normalized) {
		return true
	}
	return false
}

func sqlHasDateRestriction(sqlText string) bool {
	lower := strings.ToLower(strings.TrimSpace(sqlText))
	if lower == "" {
		return false
	}
	if strings.Contains(lower, "max(stat_date)") ||
		strings.Contains(lower, "generate_series") {
		return true
	}
	dateFilterRe := regexp.MustCompile(`(?is)\b(where|and|on)\b[^;]*(stat_date|order_timestamp|tender_timestamp|driverdone_timestamp|clientcancel_timestamp|drivercancel_timestamp)\s*(between|>=|<=|>|<|=)`)
	return dateFilterRe.MatchString(lower)
}

func sqlLooksCompatibleWithIntent(sqlText string, intent shared.Intent) bool {
	normalizedSQL := " " + strings.ToLower(strings.TrimSpace(sqlText)) + " "
	if strings.TrimSpace(sqlText) == "" {
		return false
	}
	if !strings.Contains(normalizedSQL, " metric_value ") && !regexp.MustCompile(`(?i)\bas\s+metric_value\b`).MatchString(sqlText) {
		return false
	}

	groupBy := strings.ToLower(strings.TrimSpace(intent.GroupBy))
	if groupBy != "" && (strings.Contains(normalizedSQL, " concat(") || strings.Contains(normalizedSQL, "||")) {
		return false
	}
	if strings.Contains(normalizedSQL, "select max_date from bounds where max_date < date") ||
		strings.Contains(normalizedSQL, "select max_date from bounds where max_date <=") {
		return false
	}
	if strings.Contains(normalizedSQL, " city is not null") ||
		strings.Contains(normalizedSQL, " status_order is not null") ||
		strings.Contains(normalizedSQL, " status_tender is not null") {
		return false
	}
	switch groupBy {
	case "day", "week", "month":
		return strings.Contains(normalizedSQL, " period_value ") || regexp.MustCompile(`(?i)\bas\s+period_value\b`).MatchString(sqlText)
	case "city", "status_order", "status_tender":
		return strings.Contains(normalizedSQL, " group_value ") || regexp.MustCompile(`(?i)\bas\s+group_value\b`).MatchString(sqlText)
	default:
		return true
	}
}

func preferBuilderSQLForIntent(text string, intent shared.Intent, modelSQL string) (string, bool) {
	if !shouldBuildSQLFromIntent(intent) {
		return strings.TrimSpace(modelSQL), false
	}
	if shared.ShouldBuildDomainMetricFromDetail(text, intent) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeComparisonRequest(text) && sqlLooksLikeDistributionResult(modelSQL) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeDistributionRequest(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeCheapestDailyTripsRequest(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeRollingCityMovementComparison(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeRollingMultiMetricComparison(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeCityMultiMetricComparison(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeWeekdayBestWorstRequest(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if shared.LooksLikeLatestMonthHalfComparison(text) || shared.LooksLikeNamedWeekComparison(text) {
		builderSQL, err := shared.BuildSQLFromIntent(text, intent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if !userSpecifiedPeriod(text) && sqlHasDateRestriction(modelSQL) {
		cleanIntent := normalizeImplicitPeriod(text, intent)
		builderSQL, err := shared.BuildSQLFromIntent(text, cleanIntent)
		if err == nil && strings.TrimSpace(builderSQL) != "" {
			return strings.TrimSpace(builderSQL), true
		}
	}
	if sqlLooksCompatibleWithIntent(modelSQL, intent) {
		return strings.TrimSpace(modelSQL), false
	}

	builderSQL, err := shared.BuildSQLFromIntent(text, intent)
	if err != nil || strings.TrimSpace(builderSQL) == "" {
		return strings.TrimSpace(modelSQL), false
	}
	return strings.TrimSpace(builderSQL), true
}

func sanitizeRebuiltIntentAssumptions(assumptions []string) []string {
	cleaned := make([]string, 0, len(assumptions))
	for _, assumption := range assumptions {
		normalized := shared.NormalizeText(assumption)
		if strings.Contains(normalized, "без разбивк") ||
			strings.Contains(normalized, "не позволяет разбив") ||
			strings.Contains(normalized, "персональн") {
			continue
		}
		cleaned = append(cleaned, assumption)
	}
	return cleaned
}

func sqlLooksLikeDistributionResult(sqlText string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(sqlText), " "))
	return strings.Contains(normalized, "width_bucket(") ||
		strings.Contains(normalized, " as bucket_value") ||
		strings.Contains(normalized, " bucket_value ")
}
