package shared

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const QueryRowLimit = 100

// GuardrailError помечает доменную ошибку для преобразования в ответ клиенту.
type GuardrailError struct {
	Code    string
	Message string
}

// Error выполняет отдельный шаг окружающего сервисного сценария.
func (err *GuardrailError) Error() string {
	return err.Message
}

// guardrailError выполняет отдельный шаг окружающего сервисного сценария.
func guardrailError(code, message string) error {
	return &GuardrailError{Code: code, Message: message}
}

// SQLGenerationRequest описывает JSON-запрос на границе API.
type SQLGenerationRequest struct {
	Text          string        `json:"text"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
}

// SQLGenerationResponse описывает JSON-ответ, возвращаемый вызывающей стороне.
type SQLGenerationResponse struct {
	SQL      string `json:"sql"`
	Intent   Intent `json:"intent"`
	Provider string `json:"provider"`
}

// ValidateGeneratedSQL проверяет доменные ограничения до записи или выполнения.
func ValidateGeneratedSQL(sqlText string) error {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return guardrailError("empty_sql", "Пустой SQL не может быть выполнен.")
	}

	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "select ") && !strings.HasPrefix(lower, "with ") {
		return guardrailError("read_only_only", "Разрешены только read-only SELECT-запросы.")
	}
	if strings.ContainsAny(trimmed, ";") {
		return guardrailError("multiple_statements", "Разрешён только один SQL-запрос без дополнительных инструкций.")
	}
	for _, marker := range []string{"--", "/*", "*/"} {
		if strings.Contains(trimmed, marker) {
			return guardrailError("comments_blocked", "SQL-комментарии заблокированы guardrails.")
		}
	}
	for _, forbidden := range []string{
		" insert ", " update ", " delete ", " drop ", " alter ", " truncate ",
		" create ", " grant ", " revoke ", " call ", " execute ", " do ",
		" copy ", " refresh ", " merge ", " vacuum ", " analyze ",
		" union ", " intersect ", " except ", " cross join ",
		" pg_sleep ", " dblink ", " lo_import ", " lo_export ",
	} {
		if strings.Contains(" "+lower+" ", forbidden) {
			return guardrailError("forbidden_operation", "Запрос заблокирован guardrails: содержит запрещённую SQL-операцию.")
		}
	}
	for _, forbiddenFunction := range []string{"pg_sleep", "dblink", "lo_import", "lo_export"} {
		if strings.Contains(lower, forbiddenFunction) {
			return guardrailError("forbidden_operation", "Запрос заблокирован guardrails: содержит запрещённую SQL-функцию.")
		}
	}

	for _, forbiddenRef := range []string{
		" information_schema.",
		" pg_catalog.",
		" pg_toast.",
		" pg_temp.",
		" app.",
		" public.",
	} {
		if strings.Contains(lower, forbiddenRef) {
			return guardrailError("forbidden_schema", "Запрос заблокирован: используется запрещённая схема данных.")
		}
	}
	if hasWildcardSelect(trimmed) {
		return guardrailError("wildcard_select", "Запрос заблокирован guardrails: используйте явный список колонок вместо SELECT *.")
	}

	sourceRefs := regexp.MustCompile(`(?i)\b(from|join)\s+([a-zA-Z0-9_."]+)`).FindAllStringSubmatch(trimmed, -1)
	if len(sourceRefs) == 0 {
		return guardrailError("missing_source", "Запрос должен читать данные только из разрешённых аналитических источников.")
	}

	allowedSources := allowedAnalyticsSources()
	cteNames := extractCTENames(trimmed)

	hasAllowedSource := false
	for _, match := range sourceRefs {
		if len(match) < 3 {
			continue
		}
		sourceName := normalizeSQLReference(match[2])
		if allowedSources[sourceName] {
			hasAllowedSource = true
			continue
		}
		if cteNames[sourceName] {
			continue
		}
		return guardrailError("unsupported_source", "Запрос заблокирован: обращение к этой таблице или витрине не разрешено.")
	}
	if !hasAllowedSource {
		return guardrailError("allowed_sources_only", "Запрос должен использовать только разрешённые аналитические витрины.")
	}

	if err := validateAllowedColumnReferences(trimmed); err != nil {
		return err
	}
	if err := validateSensitiveIDsAreAggregated(trimmed); err != nil {
		return err
	}
	if err := validateSQLComplexity(trimmed); err != nil {
		return err
	}

	limitRe := regexp.MustCompile(`(?i)\blimit\s+(\d+)\b`)
	if matches := limitRe.FindStringSubmatch(trimmed); len(matches) == 2 {
		limit, err := strconv.Atoi(matches[1])
		if err != nil || limit <= 0 || limit > 1000 {
			return guardrailError("limit_out_of_range", "Лимит строк должен быть в диапазоне от 1 до 1000.")
		}
	}

	return nil
}

// normalizeSQLReference нормализует граничные значения перед дальнейшим использованием.
func normalizeSQLReference(value string) string {
	normalized := strings.TrimSpace(strings.ToLower(value))
	return strings.ReplaceAll(normalized, `"`, "")
}

// allowedAnalyticsSources возвращает allowlist источников, доступных модельному SQL.
func allowedAnalyticsSources() map[string]bool {
	return map[string]bool{
		"analytics.incity":                    true,
		"analytics.driver_detail":             true,
		"analytics.pass_detail":               true,
		"analytics.v_incity_orders_latest":    true,
		"analytics.v_ride_metrics":            true,
		"analytics.v_driver_daily_metrics":    true,
		"analytics.v_passenger_daily_metrics": true,
	}
}

// extractCTENames находит имена CTE, чтобы guardrails разрешал чтение из локальных WITH-блоков.
func extractCTENames(sqlText string) map[string]bool {
	result := map[string]bool{}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(sqlText)), "with ") {
		return result
	}
	cteRe := regexp.MustCompile(`(?i)(?:with|,)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\([^)]*\))?\s+as\s*\(`)
	for _, match := range cteRe.FindAllStringSubmatch(sqlText, -1) {
		if len(match) == 2 {
			result[normalizeSQLReference(match[1])] = true
		}
	}
	return result
}

// WrapQueryForExecution изолирует небольшой важный helper для общего сценария.
func WrapQueryForExecution(sqlText string) string {
	return "select * from (" + sqlText + ") as llm_query limit " + strconv.Itoa(QueryRowLimit)
}

// hasWildcardSelect централизует проверку для последующего ветвления логики.
func hasWildcardSelect(sqlText string) bool {
	normalized := regexp.MustCompile(`(?i)count\s*\(\s*\*\s*\)`).ReplaceAllString(sqlText, "count(1)")
	selectWildcardRe := regexp.MustCompile(`(?is)\bselect\s+(?:distinct\s+)?(?:[a-zA-Z_"][a-zA-Z0-9_"]*\.)?\*\s*(?:,|from\b)`)
	commaWildcardRe := regexp.MustCompile(`(?is),\s*(?:[a-zA-Z_"][a-zA-Z0-9_"]*\.)?\*\s*(?:,|from\b)`)
	return selectWildcardRe.FindStringIndex(normalized) != nil || commaWildcardRe.FindStringIndex(normalized) != nil
}

// validateAllowedColumnReferences проверяет доменные ограничения до записи или выполнения.
func validateAllowedColumnReferences(sqlText string) error {
	allowedColumns := map[string]map[string]bool{
		"analytics.incity": {
			"city_id":                      true,
			"order_id":                     true,
			"tender_id":                    true,
			"user_id":                      true,
			"driver_id":                    true,
			"offset_hours":                 true,
			"status_order":                 true,
			"status_tender":                true,
			"order_timestamp":              true,
			"tender_timestamp":             true,
			"driveraccept_timestamp":       true,
			"driverarrived_timestamp":      true,
			"driverstarttheride_timestamp": true,
			"driverdone_timestamp":         true,
			"clientcancel_timestamp":       true,
			"drivercancel_timestamp":       true,
			"order_modified_local":         true,
			"cancel_before_accept_local":   true,
			"distance_in_meters":           true,
			"duration_in_seconds":          true,
			"price_order_local":            true,
			"price_tender_local":           true,
			"price_start_local":            true,
		},
		"analytics.driver_detail": {
			"city_id":                    true,
			"driver_id":                  true,
			"tender_date_part":           true,
			"driver_reg_date":            true,
			"orders":                     true,
			"orders_cnt_with_tenders":    true,
			"orders_cnt_accepted":        true,
			"rides_count":                true,
			"rides_time_sum_seconds":     true,
			"online_time_sum_seconds":    true,
			"client_cancel_after_accept": true,
		},
		"analytics.pass_detail": {
			"city_id":                    true,
			"user_id":                    true,
			"order_date_part":            true,
			"user_reg_date":              true,
			"orders_count":               true,
			"orders_cnt_with_tenders":    true,
			"orders_cnt_accepted":        true,
			"rides_count":                true,
			"rides_time_sum_seconds":     true,
			"online_time_sum_seconds":    true,
			"client_cancel_after_accept": true,
		},
		"analytics.v_incity_orders_latest": {
			"stat_date":           true,
			"city":                true,
			"status_order":        true,
			"status_tender":       true,
			"order_id":            true,
			"tender_id":           true,
			"user_id":             true,
			"driver_id":           true,
			"distance_in_meters":  true,
			"duration_in_seconds": true,
			"final_price_local":   true,
			"completed_orders":    true,
			"cancelled_orders":    true,
			"total_orders":        true,
		},
		"analytics.v_ride_metrics": {
			"stat_date":            true,
			"city":                 true,
			"status_order":         true,
			"status_tender":        true,
			"completed_orders":     true,
			"cancelled_orders":     true,
			"total_orders":         true,
			"gross_revenue_local":  true,
			"avg_price_local":      true,
			"avg_distance_meters":  true,
			"avg_duration_seconds": true,
		},
		"analytics.v_driver_daily_metrics": {
			"stat_date":                          true,
			"city":                               true,
			"active_drivers":                     true,
			"new_drivers":                        true,
			"total_orders":                       true,
			"orders_with_tenders":                true,
			"accepted_orders":                    true,
			"completed_rides":                    true,
			"client_cancel_after_accept":         true,
			"rides_time_sum_seconds":             true,
			"online_time_sum_seconds":            true,
			"acceptance_rate":                    true,
			"completion_rate":                    true,
			"cancel_after_accept_rate":           true,
			"avg_ride_time_seconds":              true,
			"avg_online_time_seconds_per_driver": true,
		},
		"analytics.v_passenger_daily_metrics": {
			"stat_date":                             true,
			"city":                                  true,
			"active_passengers":                     true,
			"new_passengers":                        true,
			"total_orders":                          true,
			"orders_with_tenders":                   true,
			"accepted_orders":                       true,
			"completed_rides":                       true,
			"client_cancel_after_accept":            true,
			"rides_time_sum_seconds":                true,
			"online_time_sum_seconds":               true,
			"acceptance_rate":                       true,
			"completion_rate":                       true,
			"cancel_after_accept_rate":              true,
			"avg_ride_time_seconds":                 true,
			"avg_online_time_seconds_per_passenger": true,
		},
	}

	aliasRefs := regexp.MustCompile(`(?i)\b(from|join)\s+([a-zA-Z0-9_."]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_"]+))?`).FindAllStringSubmatch(sqlText, -1)
	aliasToSource := make(map[string]string)
	for _, match := range aliasRefs {
		if len(match) < 3 {
			continue
		}
		sourceName := normalizeSQLReference(match[2])
		if _, ok := allowedColumns[sourceName]; !ok {
			continue
		}
		aliasToSource[sourceName] = sourceName
		if len(match) >= 4 {
			alias := normalizeSQLReference(match[3])
			if alias != "" {
				aliasToSource[alias] = sourceName
			}
		}
	}

	columnRefs := regexp.MustCompile(`(?i)\b([a-zA-Z_"][a-zA-Z0-9_"]*)\.([a-zA-Z_"][a-zA-Z0-9_"]*)\b`).FindAllStringSubmatch(sqlText, -1)
	for _, match := range columnRefs {
		if len(match) < 3 {
			continue
		}
		alias := normalizeSQLReference(match[1])
		column := normalizeSQLReference(match[2])
		sourceName, ok := aliasToSource[alias]
		if !ok {
			continue
		}
		if !allowedColumns[sourceName][column] {
			return guardrailError("unsupported_column", fmt.Sprintf("Запрос заблокирован: колонка %s не входит в allowlist для %s.", column, sourceName))
		}
	}

	return nil
}

// validateSQLComplexity проверяет доменные ограничения до записи или выполнения.
func validateSensitiveIDsAreAggregated(sqlText string) error {
	normalized := strings.ToLower(sqlText)
	withoutAllowedCounts := regexp.MustCompile(`(?is)count\s*\(\s*distinct\s+(?:[a-zA-Z_"][a-zA-Z0-9_"]*\.)?(?:user_id|driver_id|order_id|tender_id)\s*\)`).ReplaceAllString(normalized, "count_distinct_id")
	withoutAllowedCounts = regexp.MustCompile(`(?is)count\s*\(\s*distinct\s+case\b.*?\bend\s*\)`).ReplaceAllString(withoutAllowedCounts, "count_distinct_case")
	withoutAllowedCounts = regexp.MustCompile(`(?is)concat\s*\(\s*'(?:driver|client|passenger|user)_'\s*,\s*substr\s*\(\s*encode\s*\(\s*digest\s*\(\s*(?:[a-zA-Z_"][a-zA-Z0-9_"]*\.)?(?:user_id|driver_id)\s*,\s*'sha256'\s*\)\s*,\s*'hex'\s*\)\s*,\s*1\s*,\s*12\s*\)\s*\)`).ReplaceAllString(withoutAllowedCounts, "safe_entity_label")
	sensitiveRef := regexp.MustCompile(`(?i)\b(?:[a-zA-Z_"][a-zA-Z0-9_"]*\.)?(user_id|driver_id|order_id|tender_id)\b`)

	selectRe := regexp.MustCompile(`(?is)\bselect\b(.*?)\bfrom\b`)
	for _, match := range selectRe.FindAllStringSubmatch(withoutAllowedCounts, -1) {
		if len(match) == 2 && sensitiveRef.MatchString(match[1]) {
			return guardrailError("sensitive_identifier_projection", "Запрос заблокирован: идентификаторы клиентов, водителей, заказов и тендеров можно использовать только внутри агрегатов count(distinct ...).")
		}
	}

	groupByRe := regexp.MustCompile(`(?is)\bgroup\s+by\b(.*?)(?:\border\s+by\b|\blimit\b|$)`)
	for _, match := range groupByRe.FindAllStringSubmatch(withoutAllowedCounts, -1) {
		if len(match) == 2 && sensitiveRef.MatchString(match[1]) {
			return guardrailError("sensitive_identifier_grouping", "Запрос заблокирован: нельзя группировать результат по идентификаторам клиентов, водителей, заказов или тендеров.")
		}
	}

	return nil
}

func validateSQLComplexity(sqlText string) error {
	lower := " " + strings.ToLower(sqlText) + " "
	if strings.Count(lower, " join ") > 3 {
		return guardrailError("complexity_limit", "Запрос заблокирован guardrails: конструкция слишком сложная для безопасного выполнения.")
	}
	if strings.Count(lower, " select ") > 7 {
		return guardrailError("complexity_limit", "Запрос заблокирован guardrails: оценка сложности превышает допустимый порог.")
	}
	if strings.Count(lower, " with ") > 1 || strings.Count(lower, " as (") > 4 {
		return guardrailError("complexity_limit", "Запрос заблокирован guardrails: слишком много вложенных CTE.")
	}
	return nil
}
