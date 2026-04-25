package shared

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// QueryRequest описывает JSON-запрос на границе API.
type QueryRequest struct {
	Text string `json:"text"`
}

// Filter объединяет данные, нужные окружающему рабочему процессу.
type Filter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// UnmarshalJSON принимает строгий формат фильтра и мягко нормализует частые ответы LLM.
// Qwen иногда возвращает filters как ["final_price_local > 500"] или value числом.
// API не должен падать на таком intent, потому что SQL всё равно проходит отдельные guardrails.
func (f *Filter) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		return nil
	}

	if strings.HasPrefix(trimmed, "\"") {
		var raw string
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		*f = parseLooseFilter(raw)
		return nil
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	f.Field = firstStringValue(obj, "field", "column", "name")
	f.Operator = firstStringValue(obj, "operator", "op")
	f.Value = stringifyJSONValue(firstExistingValue(obj, "value", "val", "threshold"))

	if f.Field == "" && f.Operator == "" && f.Value == "" {
		if raw := firstStringValue(obj, "filter", "expression", "condition"); raw != "" {
			*f = parseLooseFilter(raw)
		}
	}
	return nil
}

// TimeRange объединяет данные, нужные окружающему рабочему процессу.
type TimeRange struct {
	Label string `json:"label"`
	From  string `json:"from"`
	To    string `json:"to"`
	Grain string `json:"grain"`
}

// Intent хранит нормализованный аналитический intent для обмена между сервисами.
type MetricMovementCondition struct {
	Metric    string `json:"metric"`
	Direction string `json:"direction"`
}

type Intent struct {
	Pattern            string                    `json:"pattern,omitempty"`
	Metric             string                    `json:"metric"`
	GroupBy            string                    `json:"group_by"`
	Filters            []Filter                  `json:"filters,omitempty"`
	Period             TimeRange                 `json:"period"`
	Sort               string                    `json:"sort,omitempty"`
	Limit              int                       `json:"limit,omitempty"`
	MovementConditions []MetricMovementCondition `json:"movement_conditions,omitempty"`
	Clarification      string                    `json:"clarification,omitempty"`
	Assumptions        []string                  `json:"assumptions,omitempty"`
	Confidence         float64                   `json:"confidence"`
}

// UnmarshalJSON делает intent устойчивым к нестрогим LLM-ответам в поле filters.
func (i *Intent) UnmarshalJSON(data []byte) error {
	type intentJSON struct {
		Pattern            string                    `json:"pattern"`
		Metric             string                    `json:"metric"`
		GroupBy            string                    `json:"group_by"`
		Filters            json.RawMessage           `json:"filters"`
		Period             TimeRange                 `json:"period"`
		Sort               string                    `json:"sort"`
		Limit              int                       `json:"limit"`
		MovementConditions []MetricMovementCondition `json:"movement_conditions"`
		Clarification      string                    `json:"clarification"`
		Assumptions        []string                  `json:"assumptions"`
		Confidence         float64                   `json:"confidence"`
	}

	var aux intentJSON
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	filters, err := decodeLooseFilters(aux.Filters)
	if err != nil {
		return err
	}

	*i = Intent{
		Pattern:            aux.Pattern,
		Metric:             aux.Metric,
		GroupBy:            aux.GroupBy,
		Filters:            filters,
		Period:             aux.Period,
		Sort:               aux.Sort,
		Limit:              aux.Limit,
		MovementConditions: aux.MovementConditions,
		Clarification:      aux.Clarification,
		Assumptions:        aux.Assumptions,
		Confidence:         aux.Confidence,
	}
	return nil
}

func decodeLooseFilters(raw json.RawMessage) ([]Filter, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	if strings.HasPrefix(trimmed, "[") {
		var filters []Filter
		if err := json.Unmarshal(raw, &filters); err != nil {
			return nil, err
		}
		return filters, nil
	}
	if strings.HasPrefix(trimmed, "{") {
		var filter Filter
		if err := json.Unmarshal(raw, &filter); err != nil {
			return nil, err
		}
		return []Filter{filter}, nil
	}
	if strings.HasPrefix(trimmed, "\"") {
		var filterText string
		if err := json.Unmarshal(raw, &filterText); err != nil {
			return nil, err
		}
		return []Filter{parseLooseFilter(filterText)}, nil
	}
	return nil, fmt.Errorf("invalid filters value: expected array, object or string")
}

func parseLooseFilter(raw string) Filter {
	text := strings.TrimSpace(raw)
	if text == "" {
		return Filter{}
	}
	for _, op := range []string{"!=", ">=", "<=", "=", ">", "<"} {
		if idx := strings.Index(text, op); idx >= 0 {
			return Filter{
				Field:    strings.TrimSpace(text[:idx]),
				Operator: op,
				Value:    strings.Trim(strings.TrimSpace(text[idx+len(op):]), "'\""),
			}
		}
	}
	return Filter{Value: text}
}

func firstStringValue(obj map[string]any, keys ...string) string {
	return stringifyJSONValue(firstExistingValue(obj, keys...))
}

func firstExistingValue(obj map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := obj[key]; ok {
			return value
		}
	}
	return nil
}

func stringifyJSONValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return string(raw)
	}
}

// MetricDefinition описывает семантические метаданные для аналитических запросов.
type MetricDefinition struct {
	ID               string   `json:"id"`
	Title            string   `json:"title"`
	Description      string   `json:"description"`
	Format           string   `json:"format"`
	Expression       string   `json:"expression,omitempty"`
	Synonyms         []string `json:"synonyms,omitempty"`
	WeakSynonyms     []string `json:"weak_synonyms,omitempty"`
	AllowedGroupBy   []string `json:"allowed_group_by,omitempty"`
	RecommendedChart string   `json:"recommended_chart,omitempty"`
}

// DimensionDefinition описывает семантические метаданные для аналитических запросов.
type DimensionDefinition struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Column      string   `json:"column"`
	Description string   `json:"description"`
	Synonyms    []string `json:"synonyms,omitempty"`
	Values      []string `json:"values,omitempty"`
}

// BusinessTerm объединяет данные, нужные окружающему рабочему процессу.
type BusinessTerm struct {
	Term        string `json:"term"`
	Kind        string `json:"kind"`
	Canonical   string `json:"canonical"`
	Description string `json:"description"`
}

// SourceColumnDefinition описывает семантические метаданные для аналитических запросов.
type SourceColumnDefinition struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// DataSourceDefinition описывает семантические метаданные для аналитических запросов.
type DataSourceDefinition struct {
	Name        string                   `json:"name"`
	Kind        string                   `json:"kind"`
	Description string                   `json:"description"`
	Columns     []SourceColumnDefinition `json:"columns"`
}

// SemanticLayer объединяет данные, нужные окружающему рабочему процессу.
type SemanticLayer struct {
	Metrics         []MetricDefinition     `json:"metrics"`
	Dimensions      []DimensionDefinition  `json:"dimensions"`
	Terms           []BusinessTerm         `json:"terms"`
	DataSources     []DataSourceDefinition `json:"data_sources,omitempty"`
	SampleQuestions []string               `json:"sample_questions"`
}

// IntentRequest описывает JSON-запрос на границе API.
type IntentRequest struct {
	Text          string        `json:"text"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
}

// IntentResponse описывает JSON-ответ, возвращаемый вызывающей стороне.
type IntentResponse struct {
	Intent   Intent `json:"intent"`
	Provider string `json:"provider"`
}

// QueryPreview объединяет данные, нужные окружающему рабочему процессу.
type QueryPreview struct {
	Summary         string   `json:"summary"`
	MetricLabel     string   `json:"metric_label"`
	GroupByLabel    string   `json:"group_by_label,omitempty"`
	AppliedFilters  []string `json:"applied_filters,omitempty"`
	Assumptions     []string `json:"assumptions,omitempty"`
	Clarification   string   `json:"clarification,omitempty"`
	ConfidenceLabel string   `json:"confidence_label"`
}

// ParseResponse описывает JSON-ответ, возвращаемый вызывающей стороне.
type ParseResponse struct {
	Intent        Intent        `json:"intent"`
	Preview       QueryPreview  `json:"preview"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
	SQL           string        `json:"sql,omitempty"`
	Provider      string        `json:"provider,omitempty"`
}

// QueryResult передаёт табличный результат запроса в удобном для API виде.
type QueryResult struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
	Count   int        `json:"count"`
}

// ChartSpec описывает визуализацию результата для фронтенда.
type ChartSpec struct {
	Type          string   `json:"type"`
	XKey          string   `json:"x_key"`
	YKey          string   `json:"y_key"`
	SecondaryType string   `json:"secondary_type,omitempty"`
	Headline      string   `json:"headline,omitempty"`
	Subtitle      string   `json:"subtitle,omitempty"`
	Highlights    []string `json:"highlights,omitempty"`
}

// RunResponse описывает JSON-ответ, возвращаемый вызывающей стороне.
type RunResponse struct {
	Intent        Intent        `json:"intent"`
	Preview       QueryPreview  `json:"preview"`
	SQL           string        `json:"sql"`
	Result        QueryResult   `json:"result"`
	Chart         ChartSpec     `json:"chart"`
	SemanticLayer SemanticLayer `json:"semantic_layer"`
	Provider      string        `json:"provider,omitempty"`
}

// SaveReportRequest описывает JSON-запрос на границе API.
type SaveReportRequest struct {
	Name            string       `json:"name"`
	QueryText       string       `json:"query_text"`
	SQLText         string       `json:"sql_text"`
	Intent          Intent       `json:"intent"`
	Preview         QueryPreview `json:"preview"`
	Result          QueryResult  `json:"result"`
	Provider        string       `json:"provider,omitempty"`
	Source          string       `json:"source,omitempty"`
	OwnerName       string       `json:"owner_name,omitempty"`
	OwnerDepartment string       `json:"owner_department,omitempty"`
	IsPublic        bool         `json:"is_public"`
	TemplateID      *int64       `json:"template_id,omitempty"`
}

// SavedReport объединяет данные, нужные окружающему рабочему процессу.
type SavedReport struct {
	ID              int64        `json:"id"`
	Name            string       `json:"name"`
	QueryText       string       `json:"query_text"`
	SQLText         string       `json:"sql_text"`
	Intent          Intent       `json:"intent"`
	Preview         QueryPreview `json:"preview"`
	Result          QueryResult  `json:"result"`
	Provider        string       `json:"provider,omitempty"`
	Source          string       `json:"source,omitempty"`
	OwnerName       string       `json:"owner_name,omitempty"`
	OwnerDepartment string       `json:"owner_department,omitempty"`
	IsPublic        bool         `json:"is_public"`
	TemplateID      *int64       `json:"template_id,omitempty"`
	TemplateName    string       `json:"template_name,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

// ReportTemplateSchedule хранит расписание отчёта в пользовательских и системных полях.
type ReportTemplateSchedule struct {
	Enabled   bool   `json:"enabled"`
	DayOfWeek int    `json:"day_of_week,omitempty"`
	Hour      int    `json:"hour,omitempty"`
	Minute    int    `json:"minute,omitempty"`
	Timezone  string `json:"timezone,omitempty"`
	Label     string `json:"label,omitempty"`
	NextRun   string `json:"next_run,omitempty"`
}

// ReportTemplate хранит переиспользуемый аналитический запрос и метаданные запусков.
type ReportTemplate struct {
	ID              int64                  `json:"id"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description"`
	QueryText       string                 `json:"query_text"`
	OwnerName       string                 `json:"owner_name,omitempty"`
	OwnerDepartment string                 `json:"owner_department,omitempty"`
	IsPublic        bool                   `json:"is_public"`
	Schedule        ReportTemplateSchedule `json:"schedule"`
	LastRunAt       *time.Time             `json:"last_run_at,omitempty"`
	LastStatus      string                 `json:"last_status,omitempty"`
	LastErrorText   string                 `json:"last_error_text,omitempty"`
	LastResultCount int                    `json:"last_result_count,omitempty"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

// UpsertReportTemplateRequest описывает JSON-запрос на границе API.
type UpsertReportTemplateRequest struct {
	Name            string                 `json:"name"`
	Description     string                 `json:"description"`
	QueryText       string                 `json:"query_text"`
	OwnerName       string                 `json:"owner_name"`
	OwnerDepartment string                 `json:"owner_department,omitempty"`
	IsPublic        bool                   `json:"is_public"`
	Schedule        ReportTemplateSchedule `json:"schedule"`
}

// ExportReportRequest описывает JSON-запрос на границе API.
type ExportReportRequest struct {
	Name      string      `json:"name"`
	QueryText string      `json:"query_text"`
	Run       RunResponse `json:"run"`
}

// SharingSettingsRequest описывает JSON-запрос на границе API.
type SharingSettingsRequest struct {
	OwnerDepartment string `json:"owner_department,omitempty"`
	IsPublic        bool   `json:"is_public"`
}

// NormalizeText нормализует граничные значения перед дальнейшим использованием.
func NormalizeText(value string) string {
	var normalized strings.Builder
	normalized.Grow(len(value))

	lastWasSpace := true
	for _, char := range value {
		lowered := unicode.ToLower(char)
		if lowered == '\u0451' {
			lowered = '\u0435'
		}

		if unicode.IsLetter(lowered) || unicode.IsDigit(lowered) {
			normalized.WriteRune(lowered)
			lastWasSpace = false
			continue
		}

		if !lastWasSpace {
			normalized.WriteByte(' ')
			lastWasSpace = true
		}
	}

	return strings.TrimSpace(normalized.String())
}

// ConfidenceLabel выполняет отдельный шаг окружающего сервисного сценария.
func ConfidenceLabel(score float64) string {
	switch {
	case score >= 0.8:
		return "Высокая"
	case score >= 0.6:
		return "Средняя"
	default:
		return "Низкая"
	}
}

// NormalizeIntentAliases приводит частые варианты ответа LLM к каноническим id приложения.
func NormalizeIntentAliases(intent Intent) Intent {
	intent.Metric = canonicalMetricID(intent.Metric)
	intent.GroupBy = canonicalGroupByID(intent.GroupBy)
	for index := range intent.Filters {
		intent.Filters[index].Field = canonicalFilterField(intent.Filters[index].Field)
		intent.Filters[index].Operator = strings.TrimSpace(intent.Filters[index].Operator)
		intent.Filters[index].Value = strings.TrimSpace(intent.Filters[index].Value)
	}
	return intent
}

func canonicalMetricID(metric string) string {
	normalized := strings.ToLower(strings.TrimSpace(metric))
	switch normalized {
	case "price_threshold_share", "order_price_threshold_share", "price_threshold_rate", "price_share", "price_percent", "price_threshold_percent":
		return "order_price_threshold_rate"
	case "completed_rides", "unique_completed_rides", "completed_trips":
		return "completed_rides"
	case "orders", "rides":
		return "total_orders"
	case "cancelled_orders", "canceled_orders":
		return "cancellations"
	case "active_clients", "active_customers", "clients", "customers":
		return "active_passengers"
	case "new_clients", "new_customers":
		return "new_passengers"
	default:
		return normalized
	}
}

func canonicalGroupByID(groupBy string) string {
	normalized := strings.ToLower(strings.TrimSpace(groupBy))
	switch normalized {
	case "", "none", "null", "no", "без", "без разбивки", "нет", "не задано":
		return ""
	case "driver", "drivers", "driver_id", "водитель", "водители", "по водителям":
		return "driver"
	case "client", "clients", "customer", "customers", "passenger", "passengers", "user", "users", "user_id", "клиент", "клиенты", "по клиентам", "пассажир", "пассажиры", "по пассажирам", "пользователь", "пользователи", "по пользователям":
		return "client"
	default:
		return normalized
	}
}

func canonicalFilterField(field string) string {
	normalized := strings.ToLower(strings.TrimSpace(field))
	switch normalized {
	case "price", "price_local", "order_price", "price_order", "price_order_local", "стоимость", "цена":
		return "final_price_local"
	case "city_id", "город", "город_id", "id_города":
		return "city"
	default:
		return strings.TrimSpace(field)
	}
}

// MetricLabel выполняет отдельный шаг окружающего сервисного сценария.
func (intent Intent) MetricLabel(layer SemanticLayer) string {
	metricID := canonicalMetricID(intent.Metric)
	for _, metric := range layer.Metrics {
		if canonicalMetricID(metric.ID) == metricID {
			return metric.Title
		}
	}
	switch metricID {
	case "custom_sql", "qwen_sql":
		return "Показатель по запросу"
	case "order_price_threshold_rate":
		return "Процент заказов по порогу стоимости"
	case "":
		return "Показатель"
	default:
		return humanizeIdentifier(metricID)
	}
}

// GroupByLabel выполняет отдельный шаг окружающего сервисного сценария.
func (intent Intent) GroupByLabel(layer SemanticLayer) string {
	groupByID := canonicalGroupByID(intent.GroupBy)
	if groupByID == "" {
		return ""
	}
	for _, dimension := range layer.Dimensions {
		if dimension.ID == groupByID {
			return dimension.Title
		}
	}
	return humanizeIdentifier(groupByID)
}

// BuildPreview строит производный результат из intent или данных сервиса.
func BuildPreview(intent Intent, layer SemanticLayer) QueryPreview {
	intent = NormalizeIntentAliases(intent)
	filters := make([]string, 0, len(intent.Filters))
	for _, filter := range intent.Filters {
		if label := FilterLabel(filter); label != "" {
			filters = append(filters, label)
		}
	}

	periodLabel := humanPeriodLabel(intent.Period.Label)
	if periodLabel == "" {
		periodLabel = "весь доступный период"
	}

	metricLabel := metricLabelForPreview(intent, layer)
	summary := fmt.Sprintf("%s за %s", metricLabel, periodLabel)
	if label := intent.GroupByLabel(layer); label != "" {
		summary += fmt.Sprintf(", с разбивкой по %s", strings.ToLower(label))
	}
	if len(filters) > 0 {
		summary += ". Условия: " + strings.Join(filters, ", ")
	}

	return QueryPreview{
		Summary:         summary,
		MetricLabel:     metricLabel,
		GroupByLabel:    intent.GroupByLabel(layer),
		AppliedFilters:  filters,
		Assumptions:     intent.Assumptions,
		Clarification:   intent.Clarification,
		ConfidenceLabel: ConfidenceLabel(intent.Confidence),
	}
}

func metricLabelForPreview(intent Intent, layer SemanticLayer) string {
	if canonicalMetricID(intent.Metric) == "order_price_threshold_rate" {
		return priceThresholdMetricLabel(intent.Filters)
	}
	return intent.MetricLabel(layer)
}

func priceThresholdMetricLabel(filters []Filter) string {
	for _, filter := range filters {
		if canonicalFilterField(filter.Field) != "final_price_local" {
			continue
		}
		value := formatFilterValue(canonicalFilterField(filter.Field), filter.Value)
		switch strings.TrimSpace(filter.Operator) {
		case ">":
			return "Доля заказов дороже " + value
		case ">=":
			return "Доля заказов не дешевле " + value
		case "<":
			return "Доля заказов дешевле " + value
		case "<=":
			return "Доля заказов не дороже " + value
		case "=", "==":
			return "Доля заказов стоимостью " + value
		}
	}
	return "Доля заказов по порогу стоимости"
}

// FilterLabel возвращает человекочитаемую подпись фильтра для preview.
func FilterLabel(filter Filter) string {
	field := canonicalFilterField(filter.Field)
	fieldLabel := filterFieldLabel(field)
	value := formatFilterValue(field, filter.Value)
	operator := filterOperatorLabel(filter.Operator)
	if fieldLabel == "" && operator == "" {
		return strings.TrimSpace(filter.Value)
	}
	if operator == "" {
		return strings.TrimSpace(strings.Join([]string{fieldLabel, value}, " "))
	}
	return strings.TrimSpace(strings.Join([]string{fieldLabel, operator, value}, " "))
}

func filterFieldLabel(field string) string {
	switch canonicalFilterField(field) {
	case "final_price_local", "price_order_local", "price_tender_local", "price_start_local", "avg_price_local", "gross_revenue_local":
		return "Стоимость заказа"
	case "completed_orders":
		return "Завершённые заказы"
	case "cancelled_orders":
		return "Отменённые заказы"
	case "total_orders":
		return "Все заказы"
	case "city", "city_id":
		return "Город"
	case "status_order":
		return "Статус заказа"
	case "status_tender":
		return "Статус тендера"
	case "stat_date":
		return "Дата"
	default:
		return humanizeIdentifier(field)
	}
}

func filterOperatorLabel(operator string) string {
	switch strings.TrimSpace(operator) {
	case ">":
		return "выше"
	case ">=":
		return "не ниже"
	case "<":
		return "ниже"
	case "<=":
		return "не выше"
	case "=", "==":
		return "равно"
	case "!=", "<>":
		return "не равно"
	case "ilike", "like":
		return "содержит"
	default:
		return strings.TrimSpace(operator)
	}
}

func formatFilterValue(field string, value string) string {
	cleaned := strings.Trim(strings.TrimSpace(value), "'\"")
	switch canonicalFilterField(field) {
	case "final_price_local", "price_order_local", "price_tender_local", "price_start_local", "avg_price_local", "gross_revenue_local":
		if cleaned == "" {
			return ""
		}
		return cleaned + " ₽"
	default:
		return cleaned
	}
}

func humanPeriodLabel(label string) string {
	text := strings.TrimSpace(label)
	lower := strings.ToLower(text)
	switch lower {
	case "последний месяц от последней даты в бд", "последний месяц от последней даты в данных":
		return "последний месяц в данных"
	case "последние 30 дней от последней даты в бд", "последние 30 дней от последней даты в данных":
		return "последние 30 дней в данных"
	case "":
		return ""
	default:
		return text
	}
}

func humanizeIdentifier(identifier string) string {
	text := strings.TrimSpace(identifier)
	if text == "" {
		return ""
	}
	text = strings.ReplaceAll(text, "_", " ")
	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}
	for index, word := range words {
		letters := []rune(strings.ToLower(word))
		if len(letters) == 0 {
			continue
		}
		if index == 0 {
			letters[0] = unicode.ToUpper(letters[0])
		}
		words[index] = string(letters)
	}
	return strings.Join(words, " ")
}

// DefaultSemanticLayer выполняет отдельный шаг окружающего сервисного сценария.
func DefaultSemanticLayer() SemanticLayer {
	return SemanticLayer{
		Metrics: []MetricDefinition{
			{
				ID:               "completed_orders",
				Title:            "Завершенные заказы",
				Description:      "Количество завершенных заказов",
				Format:           "integer",
				Expression:       "sum(completed_orders)",
				Synonyms:         []string{"завершенные поездки", "выполненные поездки", "завершенные заказы", "выполненные заказы", "заверш", "выполнен"},
				WeakSynonyms:     []string{"поездк"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "bar",
			},
			{
				ID:               "total_orders",
				Title:            "Все заказы",
				Description:      "Количество заказов после дедупликации по order_id",
				Format:           "integer",
				Expression:       "sum(total_orders)",
				Synonyms:         []string{"все поездки", "все заказы", "количество заказов", "сколько заказов", "число заказов"},
				WeakSynonyms:     []string{"заказ"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "bar",
			},
			{
				ID:               "cancellations",
				Title:            "Отмены",
				Description:      "Количество отмененных заказов",
				Format:           "integer",
				Expression:       "sum(cancelled_orders)",
				Synonyms:         []string{"количество отмен", "сколько отмен", "отмен", "cancel"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "bar",
			},
			{
				ID:               "cancellation_rate",
				Title:            "Соотношение отмен к завершенным",
				Description:      "Отношение количества отмен к количеству завершенных заказов",
				Format:           "percent",
				Expression:       "round(sum(cancelled_orders)::numeric / nullif(sum(completed_orders), 0), 4)",
				Synonyms:         []string{"соотношение отмен к завершенным заказам", "отношение отмен к завершенным заказам", "доля отмен к завершенным заказам", "соотношение отмен и завершенных заказов", "отношение отмен к завершенным", "соотношение отмен к завершенным"},
				WeakSynonyms:     []string{"соотношение отмен", "доля отмен к завершенным"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "line",
			},
			{
				ID:               "order_price_threshold_rate",
				Title:            "Процент заказов по порогу стоимости",
				Description:      "Доля заказов, стоимость которых выше или ниже заданного порога",
				Format:           "percent",
				Synonyms:         []string{"процент заказов по стоимости", "доля заказов по стоимости", "процент заказов дороже", "доля заказов дороже"},
				AllowedGroupBy:   []string{""},
				RecommendedChart: "metric",
			},
			{
				ID:               "revenue",
				Title:            "Выручка",
				Description:      "Сумма price_order_local по завершенным заказам",
				Format:           "currency",
				Expression:       "round(sum(gross_revenue_local)::numeric, 2)",
				Synonyms:         []string{"выручк", "оборот", "доход", "денег", "деньги"},
				WeakSynonyms:     []string{"стоимость заказов"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "line",
			},
			{
				ID:               "avg_price",
				Title:            "Средняя стоимость",
				Description:      "Средняя стоимость завершенного заказа",
				Format:           "currency",
				Expression:       "round(sum(gross_revenue_local) / nullif(sum(completed_orders), 0), 2)",
				Synonyms:         []string{"средний чек", "средн чек", "средняя стоимость", "средней стоимости", "среднюю стоимость", "средняя цена", "средней цены", "среднюю цену"},
				WeakSynonyms:     []string{"стоимост", "цен"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "line",
			},
			{
				ID:               "avg_distance_meters",
				Title:            "Средняя дистанция",
				Description:      "Средняя дистанция завершенного заказа в метрах",
				Format:           "number",
				Expression:       "round(sum(avg_distance_meters * completed_orders) / nullif(sum(completed_orders), 0), 2)",
				Synonyms:         []string{"средняя дистанция", "среднее расстояние", "дистанц", "расстояни"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "line",
			},
			{
				ID:               "avg_duration_minutes",
				Title:            "Средняя длительность",
				Description:      "Средняя длительность завершенного заказа в минутах",
				Format:           "number",
				Expression:       "round(sum(avg_duration_seconds * completed_orders) / nullif(sum(completed_orders), 0) / 60.0, 2)",
				Synonyms:         []string{"средняя длительность", "среднее время", "длительност"},
				WeakSynonyms:     []string{"время поездки"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "status_order", "status_tender"},
				RecommendedChart: "line",
			},
			{
				ID:               "active_drivers",
				Title:            "Активные водители",
				Description:      "Количество активных водителей из analytics.v_driver_daily_metrics",
				Format:           "integer",
				Expression:       "sum(active_drivers)",
				Synonyms:         []string{"активные водители", "водители онлайн", "количество водителей", "число водителей", "supply"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "new_drivers",
				Title:            "Новые водители",
				Description:      "Водители, зарегистрированные в день метрики",
				Format:           "integer",
				Expression:       "sum(new_drivers)",
				Synonyms:         []string{"новые водители", "регистрации водителей", "зарегистрированные водители"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "active_passengers",
				Title:            "Активные пассажиры",
				Description:      "Количество активных пассажиров из analytics.v_passenger_daily_metrics",
				Format:           "integer",
				Expression:       "sum(active_passengers)",
				Synonyms:         []string{"активные пассажиры", "активные клиенты", "активные пользователи", "количество пассажиров", "количество клиентов", "число пассажиров", "число клиентов", "demand"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "new_passengers",
				Title:            "Новые пассажиры",
				Description:      "Пассажиры, зарегистрированные в день метрики",
				Format:           "integer",
				Expression:       "sum(new_passengers)",
				Synonyms:         []string{"новые пассажиры", "новые клиенты", "новые пользователи", "регистрации пассажиров", "регистрации клиентов", "регистрации пользователей"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "accepted_orders",
				Title:            "Принятые заказы",
				Description:      "Принятые заказы из дневных витрин водителей или пассажиров",
				Format:           "integer",
				Expression:       "sum(accepted_orders)",
				Synonyms:         []string{"принятые заказы", "акцепты", "accepted orders"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "bar",
			},
			{
				ID:               "completed_rides",
				Title:            "Завершенные поездки",
				Description:      "Завершенные поездки из дневных витрин водителей или пассажиров",
				Format:           "integer",
				Expression:       "sum(completed_rides)",
				Synonyms:         []string{"завершенные поездки водителей", "завершенные поездки пассажиров", "completed rides"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "bar",
			},
			{
				ID:               "acceptance_rate",
				Title:            "Доля принятия",
				Description:      "Принятые заказы, деленные на заказы с тендерами",
				Format:           "percent",
				Expression:       "round(sum(accepted_orders)::numeric / nullif(sum(orders_with_tenders), 0), 4)",
				Synonyms:         []string{"доля принятия", "процент принятия", "acceptance rate", "принятие заказов"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "completion_rate",
				Title:            "Доля завершения",
				Description:      "Завершенные поездки, деленные на все заказы",
				Format:           "percent",
				Expression:       "round(sum(completed_rides)::numeric / nullif(sum(total_orders), 0), 4)",
				Synonyms:         []string{"доля завершения", "процент завершения", "completion rate"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "cancel_after_accept_rate",
				Title:            "Доля отмен после принятия",
				Description:      "Отмены клиентом после принятия, деленные на принятые заказы",
				Format:           "percent",
				Expression:       "round(sum(client_cancel_after_accept)::numeric / nullif(sum(accepted_orders), 0), 4)",
				Synonyms:         []string{"отмены после принятия", "доля отмен после принятия", "процент отмен после принятия"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
			{
				ID:               "online_time_sum_seconds",
				Title:            "Время онлайн",
				Description:      "Суммарное время онлайн в секундах из дневных витрин водителей или пассажиров",
				Format:           "number",
				Expression:       "sum(online_time_sum_seconds)",
				Synonyms:         []string{"онлайн время", "время онлайн", "online time"},
				AllowedGroupBy:   []string{"day", "week", "month", "city", "driver", "client"},
				RecommendedChart: "line",
			},
		},
		Dimensions: []DimensionDefinition{
			{ID: "day", Title: "День", Column: "stat_date", Description: "Дневная гранулярность", Synonyms: []string{"динамик", "тренд", "по дням", "ежедневно", "по датам"}},
			{ID: "week", Title: "Неделя", Column: "stat_date", Description: "Недельная гранулярность", Synonyms: []string{"по неделям", "еженедельно"}},
			{ID: "month", Title: "Месяц", Column: "stat_date", Description: "Месячная гранулярность", Synonyms: []string{"по месяцам", "ежемесячно", "каждого месяца", "каждый месяц"}},
			{ID: "city", Title: "Город (ID)", Column: "city", Description: "Идентификатор города из исходного датасета", Synonyms: []string{"по город", "по регионам", "по городам", "какие города", "города", "городов"}},
			{ID: "driver", Title: "Водитель", Column: "driver_id", Description: "Псевдонимизированный водитель; используется только для агрегированной разбивки", Synonyms: []string{"по водителям", "по водителю", "с разбивкой по водителям", "водители"}},
			{ID: "client", Title: "Клиент", Column: "user_id", Description: "Псевдонимизированный клиент; используется только для агрегированной разбивки", Synonyms: []string{"по клиентам", "по клиенту", "по пассажирам", "по пользователям", "с разбивкой по клиентам", "клиенты", "пассажиры"}},
			{ID: "status_order", Title: "Статус заказа", Column: "status_order", Description: "Итоговый статус заказа", Synonyms: []string{"по статусам заказа", "по статусу заказа", "по заказ статус"}},
			{ID: "status_tender", Title: "Статус тендера", Column: "status_tender", Description: "Статус тендера или подбора водителя", Synonyms: []string{"по статусам тендера", "по статусу тендера", "по тендер статус"}},
		},
		Terms: []BusinessTerm{
			{Term: "поездки", Kind: "metric", Canonical: "completed_orders", Description: "Завершенные поездки или заказы"},
			{Term: "завершенные поездки", Kind: "metric", Canonical: "completed_orders", Description: "Количество завершенных поездок"},
			{Term: "завершенные заказы", Kind: "metric", Canonical: "completed_orders", Description: "Количество завершенных заказов"},
			{Term: "заказы", Kind: "metric", Canonical: "total_orders", Description: "Количество всех заказов"},
			{Term: "все заказы", Kind: "metric", Canonical: "total_orders", Description: "Количество всех заказов"},
			{Term: "отмены", Kind: "metric", Canonical: "cancellations", Description: "Количество отмененных заказов"},
			{Term: "соотношение отмен к завершенным заказам", Kind: "metric", Canonical: "cancellation_rate", Description: "Отношение количества отмен к завершенным заказам"},
			{Term: "отношение отмен к завершенным заказам", Kind: "metric", Canonical: "cancellation_rate", Description: "Отношение количества отмен к завершенным заказам"},
			{Term: "доля отмен к завершенным заказам", Kind: "metric", Canonical: "cancellation_rate", Description: "Доля отмен относительно завершенных заказов"},
			{Term: "выручка", Kind: "metric", Canonical: "revenue", Description: "Суммарная выручка"},
			{Term: "средний чек", Kind: "metric", Canonical: "avg_price", Description: "Средняя стоимость завершенного заказа"},
			{Term: "средняя стоимость", Kind: "metric", Canonical: "avg_price", Description: "Средняя стоимость завершенного заказа"},
			{Term: "средняя дистанция", Kind: "metric", Canonical: "avg_distance_meters", Description: "Средняя дистанция завершенного заказа"},
			{Term: "средняя длительность", Kind: "metric", Canonical: "avg_duration_minutes", Description: "Средняя длительность завершенного заказа"},
			{Term: "по городам", Kind: "dimension", Canonical: "city", Description: "Группировка по городам"},
			{Term: "по статусам заказа", Kind: "dimension", Canonical: "status_order", Description: "Группировка по статусу заказа"},
			{Term: "по статусам тендера", Kind: "dimension", Canonical: "status_tender", Description: "Группировка по статусу тендера"},
			{Term: "по дням", Kind: "dimension", Canonical: "day", Description: "Группировка по дням"},
			{Term: "по неделям", Kind: "dimension", Canonical: "week", Description: "Группировка по неделям"},
			{Term: "по месяцам", Kind: "dimension", Canonical: "month", Description: "Группировка по месяцам"},
		},
		DataSources: []DataSourceDefinition{
			{
				Name:        "analytics.incity",
				Kind:        "table",
				Description: "Сырой источник заказов и тендеров. Используй его, если нужны исходные статусы, timestamp-поля, цены или другие колонки, которых нет в агрегированных представлениях.",
				Columns: []SourceColumnDefinition{
					{Name: "city_id", Type: "text", Description: "Идентификатор города"},
					{Name: "order_id", Type: "text", Description: "Идентификатор заказа"},
					{Name: "tender_id", Type: "text", Description: "Идентификатор тендера"},
					{Name: "user_id", Type: "text", Description: "Идентификатор клиента"},
					{Name: "driver_id", Type: "text", Description: "Идентификатор водителя"},
					{Name: "offset_hours", Type: "integer", Description: "Локальное смещение часового пояса"},
					{Name: "status_order", Type: "text", Description: "Статус заказа"},
					{Name: "status_tender", Type: "text", Description: "Статус тендера"},
					{Name: "order_timestamp", Type: "timestamptz", Description: "Время создания заказа"},
					{Name: "tender_timestamp", Type: "timestamptz", Description: "Время создания тендера"},
					{Name: "driveraccept_timestamp", Type: "timestamptz", Description: "Время принятия заказа водителем"},
					{Name: "driverarrived_timestamp", Type: "timestamptz", Description: "Время прибытия водителя"},
					{Name: "driverstarttheride_timestamp", Type: "timestamptz", Description: "Время начала поездки"},
					{Name: "driverdone_timestamp", Type: "timestamptz", Description: "Время завершения поездки"},
					{Name: "clientcancel_timestamp", Type: "timestamptz", Description: "Время отмены клиентом"},
					{Name: "drivercancel_timestamp", Type: "timestamptz", Description: "Время отмены водителем"},
					{Name: "order_modified_local", Type: "timestamptz", Description: "Последнее локальное изменение заказа"},
					{Name: "cancel_before_accept_local", Type: "timestamptz", Description: "Время отмены до принятия водителем"},
					{Name: "distance_in_meters", Type: "numeric", Description: "Дистанция поездки в метрах"},
					{Name: "duration_in_seconds", Type: "integer", Description: "Длительность поездки в секундах"},
					{Name: "price_order_local", Type: "numeric", Description: "Стоимость заказа"},
					{Name: "price_tender_local", Type: "numeric", Description: "Стоимость тендера"},
					{Name: "price_start_local", Type: "numeric", Description: "Стартовая цена"},
				},
			},
			{
				Name:        "analytics.v_incity_orders_latest",
				Kind:        "view",
				Description: "Дедуплицированная order-level витрина: одно актуальное состояние на order_id. Используй, когда нужна аналитика на уровне заказа без дублей.",
				Columns: []SourceColumnDefinition{
					{Name: "stat_date", Type: "date", Description: "Дата заказа или последнего релевантного события"},
					{Name: "city", Type: "text", Description: "Идентификатор города"},
					{Name: "status_order", Type: "text", Description: "Статус заказа"},
					{Name: "status_tender", Type: "text", Description: "Статус тендера"},
					{Name: "order_id", Type: "text", Description: "Идентификатор заказа; не выводить в результат, использовать только для count distinct"},
					{Name: "tender_id", Type: "text", Description: "Идентификатор тендера; не выводить в результат"},
					{Name: "user_id", Type: "text", Description: "Идентификатор пользователя; не выводить в результат"},
					{Name: "driver_id", Type: "text", Description: "Идентификатор водителя; не выводить в результат"},
					{Name: "distance_in_meters", Type: "numeric", Description: "Дистанция поездки в метрах"},
					{Name: "duration_in_seconds", Type: "integer", Description: "Длительность поездки в секундах"},
					{Name: "final_price_local", Type: "numeric", Description: "Итоговая цена заказа"},
					{Name: "completed_orders", Type: "integer", Description: "1 если заказ завершён"},
					{Name: "cancelled_orders", Type: "integer", Description: "1 если заказ отменён"},
					{Name: "total_orders", Type: "integer", Description: "1 для каждого дедуплицированного заказа"},
				},
			},
			{
				Name:        "analytics.v_ride_metrics",
				Kind:        "view",
				Description: "Готовый агрегированный слой для безопасных аналитических запросов и визуализаций.",
				Columns: []SourceColumnDefinition{
					{Name: "stat_date", Type: "date", Description: "Дата агрегирования"},
					{Name: "city", Type: "text", Description: "Идентификатор города"},
					{Name: "status_order", Type: "text", Description: "Статус заказа"},
					{Name: "status_tender", Type: "text", Description: "Статус тендера"},
					{Name: "completed_orders", Type: "integer", Description: "Количество завершенных заказов"},
					{Name: "cancelled_orders", Type: "integer", Description: "Количество отмененных заказов"},
					{Name: "total_orders", Type: "integer", Description: "Количество всех заказов"},
					{Name: "gross_revenue_local", Type: "numeric", Description: "Выручка по завершенным заказам"},
					{Name: "avg_price_local", Type: "numeric", Description: "Средняя стоимость заказа"},
					{Name: "avg_distance_meters", Type: "numeric", Description: "Средняя дистанция в метрах"},
					{Name: "avg_duration_seconds", Type: "numeric", Description: "Средняя длительность в секундах"},
				},
			},
			{
				Name:        "analytics.v_driver_daily_metrics",
				Kind:        "view",
				Description: "Aggregated daily driver activity by city. Use for driver supply, acceptance, completion, cancellation-after-accept, online time and ride time metrics.",
				Columns: []SourceColumnDefinition{
					{Name: "stat_date", Type: "date", Description: "Metric date"},
					{Name: "city", Type: "text", Description: "City identifier"},
					{Name: "active_drivers", Type: "integer", Description: "Drivers with daily activity"},
					{Name: "new_drivers", Type: "integer", Description: "Drivers whose registration date equals the metric date"},
					{Name: "total_orders", Type: "integer", Description: "Orders associated with drivers"},
					{Name: "orders_with_tenders", Type: "integer", Description: "Orders with tenders"},
					{Name: "accepted_orders", Type: "integer", Description: "Accepted orders"},
					{Name: "completed_rides", Type: "integer", Description: "Completed rides"},
					{Name: "client_cancel_after_accept", Type: "integer", Description: "Client cancellations after driver acceptance"},
					{Name: "rides_time_sum_seconds", Type: "numeric", Description: "Total ride time in seconds"},
					{Name: "online_time_sum_seconds", Type: "numeric", Description: "Total driver online time in seconds"},
					{Name: "acceptance_rate", Type: "numeric", Description: "Accepted orders divided by orders with tenders"},
					{Name: "completion_rate", Type: "numeric", Description: "Completed rides divided by total orders"},
					{Name: "cancel_after_accept_rate", Type: "numeric", Description: "Client cancellations after accept divided by accepted orders"},
					{Name: "avg_ride_time_seconds", Type: "numeric", Description: "Average completed ride time in seconds"},
					{Name: "avg_online_time_seconds_per_driver", Type: "numeric", Description: "Average online time per active driver in seconds"},
				},
			},
			{
				Name:        "analytics.v_passenger_daily_metrics",
				Kind:        "view",
				Description: "Aggregated daily passenger activity by city. Use for demand, accepted orders, completed rides, cancellation-after-accept, online time and ride time metrics.",
				Columns: []SourceColumnDefinition{
					{Name: "stat_date", Type: "date", Description: "Metric date"},
					{Name: "city", Type: "text", Description: "City identifier"},
					{Name: "active_passengers", Type: "integer", Description: "Passengers with daily activity"},
					{Name: "new_passengers", Type: "integer", Description: "Passengers whose registration date equals the metric date"},
					{Name: "total_orders", Type: "integer", Description: "Passenger orders"},
					{Name: "orders_with_tenders", Type: "integer", Description: "Orders with tenders"},
					{Name: "accepted_orders", Type: "integer", Description: "Accepted orders"},
					{Name: "completed_rides", Type: "integer", Description: "Completed rides"},
					{Name: "client_cancel_after_accept", Type: "integer", Description: "Client cancellations after driver acceptance"},
					{Name: "rides_time_sum_seconds", Type: "numeric", Description: "Total ride time in seconds"},
					{Name: "online_time_sum_seconds", Type: "numeric", Description: "Total passenger online time in seconds"},
					{Name: "acceptance_rate", Type: "numeric", Description: "Accepted orders divided by orders with tenders"},
					{Name: "completion_rate", Type: "numeric", Description: "Completed rides divided by total orders"},
					{Name: "cancel_after_accept_rate", Type: "numeric", Description: "Client cancellations after accept divided by accepted orders"},
					{Name: "avg_ride_time_seconds", Type: "numeric", Description: "Average completed ride time in seconds"},
					{Name: "avg_online_time_seconds_per_passenger", Type: "numeric", Description: "Average online time per active passenger in seconds"},
				},
			},
		},
		SampleQuestions: []string{
			"Покажи выручку по городам за последние 30 дней",
			"Сколько было отмен по статусам заказа на прошлой неделе",
			"Покажи среднюю стоимость по дням за последние 14 дней",
			"Сколько завершенных заказов по статусам тендера за месяц",
		},
	}
}

// MustJSON изолирует небольшой важный helper для общего сценария.
func MustJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
