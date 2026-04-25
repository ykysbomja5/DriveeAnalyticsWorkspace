package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"drivee-self-service/internal/shared"
)

type queryHints struct {
	Normalized      string
	Metric          string
	MetricReason    string
	GroupBys        []string
	Pattern         string
	OutputShape     string
	ExpectedColumns string
	Visualization   string
	Limit           int
	PriceOperator   string
	PriceThreshold  string
	PeriodSpecified bool
	Notes           []string
}

func buildQwenUserMessage(text string) string {
	trimmed := strings.TrimSpace(text)
	hints := inferQueryHints(trimmed)

	var builder strings.Builder
	builder.WriteString("Пользовательский запрос:\n")
	builder.WriteString(trimmed)
	builder.WriteString("\n\n")
	builder.WriteString("Backend-подсказки по пониманию запроса. Это не готовый SQL, а стабилизирующие подсказки для русского NL2SQL; если они конфликтуют с явно написанным пользователем, следуй пользовательскому тексту.\n")
	builder.WriteString(hints.format())
	return builder.String()
}

func inferQueryHints(text string) queryHints {
	normalized := shared.NormalizeText(text)
	hints := queryHints{Normalized: normalized}

	hints.Metric, hints.MetricReason = inferMetric(normalized)
	hints.GroupBys = inferGroupBys(normalized)
	hints.Pattern = inferPattern(normalized)
	hints.Limit = inferLimit(normalized)
	hints.PriceOperator, hints.PriceThreshold = inferPriceThreshold(normalized)
	hints.PeriodSpecified = hintUserSpecifiedPeriod(normalized)

	hints.OutputShape, hints.ExpectedColumns, hints.Visualization = inferOutputShape(hints)
	hints.Notes = inferNotes(normalized, hints)
	return hints
}

func (h queryHints) format() string {
	lines := []string{
		fmt.Sprintf("- Нормализованный текст: %q", h.Normalized),
	}
	if h.Metric != "" {
		lines = append(lines, fmt.Sprintf("- Вероятная метрика: %s (%s)", h.Metric, h.MetricReason))
	}
	if len(h.GroupBys) > 0 {
		lines = append(lines, fmt.Sprintf("- Вероятные разрезы: %s", strings.Join(h.GroupBys, ", ")))
	}
	if h.Pattern != "" {
		lines = append(lines, fmt.Sprintf("- Вероятный сценарий: %s", h.Pattern))
	}
	if h.Limit > 0 {
		lines = append(lines, fmt.Sprintf("- Вероятный limit/top N: %d", h.Limit))
	}
	if h.PriceOperator != "" && h.PriceThreshold != "" {
		lines = append(lines, fmt.Sprintf("- Ценовой фильтр для intent.filters: {\"field\":\"final_price_local\",\"operator\":%q,\"value\":%q}", h.PriceOperator, h.PriceThreshold))
	}
	if h.OutputShape != "" {
		lines = append(lines, fmt.Sprintf("- Ожидаемая форма результата: %s", h.OutputShape))
	}
	if h.ExpectedColumns != "" {
		lines = append(lines, fmt.Sprintf("- Ожидаемые SQL aliases: %s", h.ExpectedColumns))
	}
	if h.Visualization != "" {
		lines = append(lines, fmt.Sprintf("- Предпочтительная визуализация по форме ответа: %s", h.Visualization))
	}
	if !h.PeriodSpecified {
		lines = append(lines, "- Период в запросе явно не указан: не используй последние 7/14/30 дней по умолчанию, считай по всему доступному периоду в базе; в intent.period верни label \"весь доступный период\", from/to пустые.")
	}
	for _, note := range h.Notes {
		lines = append(lines, "- "+note)
	}
	return strings.Join(lines, "\n")
}

func inferMetric(text string) (string, string) {
	if text == "" {
		return "", ""
	}

	if hasAny(text, "процент", "процентов", "доля", "сколько процентов") &&
		hasAny(text, "стоимост", "цен", "руб", "дороже", "дешевле") &&
		hasAny(text, "выше", "ниже", "больше", "меньше", "дороже", "дешевле") {
		return "order_price_threshold_rate", "процент/доля заказов относительно ценового порога"
	}
	if hasAny(text, "средн") && hasAny(text, "чек", "стоимост", "цен") {
		return "avg_price", "средняя стоимость/средний чек"
	}
	if hasAny(text, "средн") && hasAny(text, "дистанц", "расстоян") {
		return "avg_distance_meters", "средняя дистанция"
	}
	if hasAny(text, "средн") && hasAny(text, "длительност", "время") {
		return "avg_duration_minutes", "средняя длительность"
	}
	if hasAny(text, "соотношение", "отношение", "доля", "процент") && hasAny(text, "отмен") {
		return "cancellation_rate", "доля/отношение отмен"
	}
	if hasAny(text, "отмен") {
		return "cancellations", "количество отмен"
	}
	if hasAny(text, "выручк", "оборот", "доход", "деньг", "gmv") {
		return "revenue", "выручка/оборот"
	}
	if hasAny(text, "заверш", "выполн") || (hasAny(text, "поездк") && !hasAny(text, "все поезд")) {
		return "completed_orders", "завершённые поездки/заказы"
	}
	if hasAny(text, "заказ", "поездк") {
		return "total_orders", "все заказы/поездки"
	}
	return "", ""
}

func inferGroupBys(text string) []string {
	groups := make([]string, 0, 2)
	if isDistributionText(text) {
		return groups
	}
	if hasAny(text, "по месяцам", "помесяч", "ежемесяч", "каждый месяц", "каждого месяца") {
		groups = appendUnique(groups, "month")
	} else if hasAny(text, "по недел", "еженедель") {
		groups = appendUnique(groups, "week")
	} else if hasAny(text, "по дням", "по датам", "ежеднев", "динамик", "тренд", "график") {
		groups = appendUnique(groups, "day")
	}

	if hasAny(text, "по город", "по регионам", "городов", "города") {
		groups = appendUnique(groups, "city")
	}
	if hasAny(text, "по статусам заказа", "по статусу заказа", "статус заказа") {
		groups = appendUnique(groups, "status_order")
	}
	if hasAny(text, "по статусам тендера", "по статусу тендера", "статус тендера", "тендер статус") {
		groups = appendUnique(groups, "status_tender")
	}
	return groups
}

func inferPattern(text string) string {
	if hasAny(text, "сравни", "сравнение", "сравнить", "против", "versus", "vs", "предыдущ") {
		return "comparison"
	}
	if isDistributionText(text) {
		return "distribution"
	}
	if hasAny(text, "динамик", "тренд", "график") {
		return "metric_timeseries"
	}
	return ""
}

func inferLimit(text string) int {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?:топ|top)\s*[- ]?\s*(\d{1,3})`),
		regexp.MustCompile(`(?:первые|первую)\s+(\d{1,3})`),
	}
	for _, pattern := range patterns {
		match := pattern.FindStringSubmatch(text)
		if len(match) != 2 {
			continue
		}
		limit, err := strconv.Atoi(match[1])
		if err == nil && limit > 0 && limit <= 1000 {
			return limit
		}
	}
	return 0
}

func inferPriceThreshold(text string) (string, string) {
	if !hasAny(text, "стоимост", "цен", "руб", "дороже", "дешевле") {
		return "", ""
	}
	operator := ""
	switch {
	case hasAny(text, "ниже", "меньше", "дешевле", "до "):
		operator = "<"
	case hasAny(text, "выше", "больше", "дороже", "от "):
		operator = ">"
	default:
		return "", ""
	}

	number := thresholdNumberNearOperator(text, operator)
	if number == "" {
		number = regexp.MustCompile(`\d+(?:[,.]\d+)?`).FindString(text)
	}
	if number == "" {
		return "", ""
	}
	return operator, strings.ReplaceAll(number, ",", ".")
}

func thresholdNumberNearOperator(text, operator string) string {
	var pattern *regexp.Regexp
	if operator == ">" {
		pattern = regexp.MustCompile(`(?:выше|больше|дороже|от)\s+(\d+(?:[,.]\d+)?)`)
	} else {
		pattern = regexp.MustCompile(`(?:ниже|меньше|дешевле|до)\s+(\d+(?:[,.]\d+)?)`)
	}
	if match := pattern.FindStringSubmatch(text); len(match) == 2 {
		return match[1]
	}
	return ""
}

func inferOutputShape(h queryHints) (string, string, string) {
	if hasAny(h.Normalized, "таблиц", "строк", "исходн", "выгруз") {
		return "табличный ответ с явно перечисленными колонками", "явные имена колонок без select *", "table"
	}

	hasTime := hasGroup(h.GroupBys, "day") || hasGroup(h.GroupBys, "week") || hasGroup(h.GroupBys, "month")
	hasCategory := hasGroup(h.GroupBys, "city") || hasGroup(h.GroupBys, "status_order") || hasGroup(h.GroupBys, "status_tender")
	if isMultiMetricRequest(h.Normalized) {
		return "сравнение периодов с несколькими метриками", "period_value, metric_value, revenue_value, completed_orders_value, cancellations_value, avg_price_value, cancellation_rate_value", "table"
	}

	switch {
	case h.Pattern == "distribution":
		return "распределение по интервалам", "bucket_value, metric_value", "histogram"
	case hasTime && hasCategory:
		return "временной ряд с категориальной разбивкой", "period_value, group_value, metric_value", "line + structure"
	case hasTime:
		return "временной ряд", "period_value, metric_value", "area-line"
	case hasCategory:
		return "категориальная разбивка", "group_value, metric_value", "bar или lollipop"
	case h.Pattern == "comparison":
		return "сравнение периодов/сегментов", "period_value, metric_value", "bar"
	default:
		return "один KPI без разбивки", "metric_value", "metric"
	}
}

func inferNotes(text string, h queryHints) []string {
	notes := make([]string, 0, 3)
	if h.Pattern == "distribution" {
		notes = append(notes, "Запрос похож на распределение: период используй только как фильтр, а осью X должны быть интервалы значений; не строй временной ряд и не считай среднее вместо частот.")
	}
	if hasAny(text, "график", "динамик", "тренд") && !hasAny(text, "по месяцам", "по недел", "по дням") && len(h.GroupBys) == 1 && h.GroupBys[0] == "day" {
		notes = append(notes, "Слово «график» без другого разреза обычно означает динамику по дням: не подменяй её гистограммой или категориальной разбивкой.")
	}
	if hasAny(text, "гистограмм") {
		notes = append(notes, "Слово «гистограмма» трактуй как предпочтение визуализации. Не меняй метрику из-за него; SQL-форма всё равно должна быть metric_value/period_value/group_value.")
	}
	if h.Metric == "order_price_threshold_rate" {
		notes = append(notes, "Для ценового порога считай процент заказов по order-level витрине analytics.v_incity_orders_latest, а не среднюю стоимость и не выручку.")
	}
	return notes
}

func isMultiMetricRequest(text string) bool {
	metricHits := 0
	if hasAny(text, "выручк", "оборот", "доход") {
		metricHits++
	}
	if hasAny(text, "заверш", "выполнен") {
		metricHits++
	}
	if hasAny(text, "отмен") {
		metricHits++
	}
	if hasAny(text, "средн") && hasAny(text, "стоимост", "цен", "чек") {
		metricHits++
	}
	if hasAny(text, "доля отмен", "процент отмен", "соотношение отмен") {
		metricHits++
	}
	return metricHits >= 3
}

func isDistributionText(text string) bool {
	return shared.LooksLikeDistributionRequest(text)
}

func hintUserSpecifiedPeriod(text string) bool {
	if text == "" {
		return false
	}
	if regexp.MustCompile(`(^| )20\d{2}( |$)`).MatchString(text) ||
		regexp.MustCompile(`(^| )\d{4}\s+\d{2}\s+\d{2}( |$)`).MatchString(text) {
		return true
	}
	if hasAny(text, "последн", "прошл", "текущ", "сегодня", "вчера", "позавчера") {
		return true
	}
	if regexp.MustCompile(`(^| )за\s+\d+\s+(дн|недел|месяц|мес|год)`).MatchString(text) {
		return true
	}
	if regexp.MustCompile(`(^| )(за|в|на)\s+(день|недел|месяц|квартал|год|январ|феврал|март|апрел|ма[ийя]|июн|июл|август|сентябр|октябр|ноябр|декабр)`).MatchString(text) {
		return true
	}
	if regexp.MustCompile(`(^| )с\s+\d{1,2}\s+по\s+\d{1,2}( |$)`).MatchString(text) {
		return true
	}
	return false
}

func hasAny(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, needle) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func hasGroup(groups []string, group string) bool {
	for _, candidate := range groups {
		if candidate == group {
			return true
		}
	}
	return false
}
