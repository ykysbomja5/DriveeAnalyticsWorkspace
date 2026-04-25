package shared

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// monthEdgeComparison объединяет данные, нужные окружающему рабочему процессу.
type monthEdgeComparison struct {
	FirstDays   int
	LastDays    int
	Year        int
	Month       time.Month
	FirstFrom   string
	FirstTo     string
	FirstLabel  string
	LastFrom    string
	LastTo      string
	LastLabel   string
	PeriodLabel string
}

// multiMonthComparison объединяет данные, нужные окружающему рабочему процессу.
type multiMonthComparison struct {
	Months []time.Month
	Year   int
	From   string
	To     string
	Label  string
}

// monthlyWindowComparison объединяет данные, нужные окружающему рабочему процессу.
type monthlyWindowComparison struct {
	Days   int
	Months []time.Month
	Year   int
	From   string
	To     string
	Label  string
}

// OrderPriceThresholdShare describes a share of orders above/below a price threshold.
type OrderPriceThresholdShare struct {
	Threshold float64
	Operator  string
}

type metricSource struct {
	Table       string
	Alias       string
	DateColumn  string
	CityColumn  string
	EntityID    string
	RegDate     string
	Orders      string
	WithTenders string
	Accepted    string
	Completed   string
	OnlineTime  string
	RideTime    string
	CancelAfter string
	Kind        string
}

// DetectMonthlyWindowComparisonRange выводит аналитическое поведение из текста пользователя или результата.
func DetectMonthlyWindowComparisonRange(text string) (TimeRange, bool) {
	comparison, ok := detectMonthlyWindowComparison(text)
	if !ok {
		return TimeRange{}, false
	}
	return TimeRange{
		Label: comparison.Label,
		From:  comparison.From,
		To:    comparison.To,
		Grain: "month",
	}, true
}

// DetectMonthEdgeComparisonRange выводит аналитическое поведение из текста пользователя или результата.
func DetectMonthEdgeComparisonRange(text string) (TimeRange, bool) {
	if comparison, ok := detectNamedWeekComparison(text); ok {
		return TimeRange{
			Label: comparison.PeriodLabel,
			From:  comparison.FirstFrom,
			To:    comparison.LastTo,
			Grain: "week",
		}, true
	}
	comparison, ok := detectMonthEdgeComparison(text)
	if !ok {
		return TimeRange{}, false
	}
	return TimeRange{
		Label: comparison.PeriodLabel,
		From:  comparison.FirstFrom,
		To:    comparison.LastTo,
		Grain: "day",
	}, true
}

// DetectMultiMonthComparisonRange выводит аналитическое поведение из текста пользователя или результата.
func DetectMultiMonthComparisonRange(text string) (TimeRange, bool) {
	comparison, ok := detectMultiMonthComparison(text)
	if !ok {
		return TimeRange{}, false
	}
	return TimeRange{
		Label: comparison.Label,
		From:  comparison.From,
		To:    comparison.To,
		Grain: "month",
	}, true
}

// BuildSQLFromIntent строит производный результат из intent или данных сервиса.
func BuildSQLFromIntent(text string, intent Intent) (string, error) {
	intent = NormalizeEntityGroupBy(text, intent)

	if sqlText, ok := buildRollingCityMovementComparisonSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildRollingMultiMetricComparisonSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildCityMultiMetricComparisonSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildWeekdayBestWorstSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildLatestMonthHalfComparisonSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildRollingPreviousPeriodComparisonSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildDistributionSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildMetricThresholdDaysSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildOrderPriceThresholdShareSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildCheapestDailyTripsSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if sqlText, ok := buildMetricMovementFilterSQL(text, intent); ok {
		return compactSQL(sqlText), nil
	}

	if comparison, sqlText, ok := buildMonthlyWindowComparisonSQL(text, intent); ok {
		intent.Period = TimeRange{
			Label: comparison.Label,
			From:  comparison.From,
			To:    comparison.To,
			Grain: "month",
		}
		_ = intent
		return compactSQL(sqlText), nil
	}

	if comparison, sqlText, ok := buildSpecialComparisonSQL(text, intent); ok {
		intent.Period = TimeRange{
			Label: comparison.PeriodLabel,
			From:  comparison.FirstFrom,
			To:    comparison.LastTo,
			Grain: "day",
		}
		_ = intent
		return compactSQL(sqlText), nil
	}

	if comparison, sqlText, ok := buildSpecialMultiMonthComparisonSQL(text, intent); ok {
		intent.Period = TimeRange{
			Label: comparison.Label,
			From:  comparison.From,
			To:    comparison.To,
			Grain: "month",
		}
		_ = intent
		return compactSQL(sqlText), nil
	}

	return buildIntentSQL(text, intent)
}

// LooksLikeDistributionRequest detects requests where the user wants a histogram/distribution,
// not a time series or a single average.
func LooksLikeDistributionRequest(text string) bool {
	normalized := NormalizeText(text)
	hasDistributionWord := strings.Contains(normalized, "распредел") ||
		strings.Contains(normalized, "диапазон") ||
		strings.Contains(normalized, "бакет") ||
		strings.Contains(normalized, "интервал") ||
		strings.Contains(normalized, "частот")
	if hasDistributionWord {
		return true
	}

	if !strings.Contains(normalized, "гистограмм") {
		return false
	}
	if LooksLikeComparisonRequest(normalized) {
		return false
	}
	return !strings.Contains(normalized, "средн")
}

// LooksLikeComparisonRequest detects requests where values should be compared as
// named periods or segments, even if the user asks to render the result as bars.
func LooksLikeComparisonRequest(text string) bool {
	normalized := NormalizeText(text)
	return strings.Contains(normalized, "сравн") ||
		strings.Contains(normalized, "против") ||
		strings.Contains(normalized, "предыдущ") ||
		strings.Contains(normalized, " vs ") ||
		strings.Contains(normalized, " versus ")
}

func buildDistributionSQL(text string, intent Intent) (string, bool) {
	normalized := NormalizeText(text)
	if !LooksLikeDistributionRequest(normalized) {
		return "", false
	}

	field, _, ok := distributionField(normalized, intent)
	if !ok {
		return "", false
	}

	period := intent.Period
	if !hasValidDateBounds(period.From, period.To) {
		if detected, ok := DetectCalendarMonthRange(text); ok {
			period = detected
		}
	}

	whereClauses := []string{fmt.Sprintf("o.%s is not null", field)}
	if hasValidDateBounds(period.From, period.To) {
		whereClauses = append(whereClauses, fmt.Sprintf("o.stat_date between date %s and date %s", quoteSQLLiteral(period.From), quoteSQLLiteral(period.To)))
	}
	if strings.Contains(normalized, "заверш") || strings.Contains(normalized, "выполн") {
		whereClauses = append(whereClauses, "o.completed_orders = 1")
	}
	whereSQL := strings.Join(whereClauses, " and ")

	return fmt.Sprintf(`
with stats as (
  select
    min(o.%[1]s)::numeric as min_value,
    max(o.%[1]s)::numeric as max_value
  from analytics.v_incity_orders_latest o
  where %[2]s
),
bucketed as (
  select
    width_bucket(o.%[1]s::numeric, s.min_value, s.max_value + 0.000001, 10) as bucket_number,
    s.min_value,
    s.max_value
  from analytics.v_incity_orders_latest o
  join stats s on true
  where %[2]s
    and s.min_value is not null
    and s.max_value is not null
)
select
  concat(
    round((min_value + (bucket_number - 1) * ((max_value - min_value) / 10.0))::numeric, 2),
    ' - ',
    round((min_value + bucket_number * ((max_value - min_value) / 10.0))::numeric, 2)
  ) as bucket_value,
  count(*)::integer as metric_value
from bucketed
where bucket_number between 1 and 10
group by bucket_number, min_value, max_value
order by bucket_number
limit 100
`, field, whereSQL), true
}

func distributionField(normalized string, intent Intent) (string, string, bool) {
	metric := canonicalMetricID(intent.Metric)
	switch {
	case strings.Contains(normalized, "дистанц") || strings.Contains(normalized, "расстоян") || metric == "avg_distance_meters":
		return "distance_in_meters", "дистанция", true
	case strings.Contains(normalized, "длитель") || strings.Contains(normalized, "время") || metric == "avg_duration_minutes":
		return "duration_in_seconds", "длительность", true
	case strings.Contains(normalized, "цен") ||
		strings.Contains(normalized, "стоимост") ||
		strings.Contains(normalized, "чек") ||
		strings.Contains(normalized, "заказ") ||
		metric == "avg_price" ||
		metric == "order_price_threshold_rate":
		return "final_price_local", "стоимость", true
	default:
		return "", "", false
	}
}

func buildMetricThresholdDaysSQL(text string, intent Intent) (string, bool) {
	normalized := NormalizeText(text)
	if normalized == "" {
		return "", false
	}
	if !strings.Contains(normalized, "дн") && intent.GroupBy != "day" {
		return "", false
	}
	operator, threshold, ok := detectMetricThreshold(normalized)
	if !ok {
		return "", false
	}

	metric := canonicalMetricID(intent.Metric)
	if metric == "" {
		if strings.Contains(normalized, "средн") && (strings.Contains(normalized, "цен") || strings.Contains(normalized, "стоимост") || strings.Contains(normalized, "чек")) {
			metric = "avg_price"
		}
	}
	metricExpr, ok := metricExpressionWithAlias(metric, "vm")
	if !ok {
		return "", false
	}
	if metric == "avg_price" && shouldUseOrderLevelAveragePrice(normalized) {
		return buildOrderLevelAveragePriceThresholdDaysSQL(text, intent, operator, threshold)
	}

	period := intent.Period
	if !hasValidDateBounds(period.From, period.To) {
		if detected, ok := DetectCalendarMonthRange(text); ok {
			period = detected
		}
	}

	whereClauses := buildWhereClauses("vm", Intent{Period: period, Filters: intent.Filters}, true)
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "where " + strings.Join(whereClauses, " and ")
	}

	return fmt.Sprintf(`
select
  vm.stat_date as period_value,
  %s as metric_value
from analytics.v_ride_metrics vm
%s
group by vm.stat_date
having %s %s %s
order by period_value
limit 100
`, metricExpr, whereSQL, metricExpr, operator, strconv.FormatFloat(threshold, 'f', -1, 64)), true
}

func shouldUseOrderLevelAveragePrice(normalized string) bool {
	if strings.Contains(normalized, "средн") && (strings.Contains(normalized, "цена") || strings.Contains(normalized, "стоимост")) {
		return true
	}
	return false
}

func buildOrderLevelAveragePriceThresholdDaysSQL(text string, intent Intent, operator string, threshold float64) (string, bool) {
	period := intent.Period
	if !hasValidDateBounds(period.From, period.To) {
		if detected, ok := DetectCalendarMonthRange(text); ok {
			period = detected
		}
	}

	whereClauses := make([]string, 0, 3)
	if hasValidDateBounds(period.From, period.To) {
		whereClauses = append(whereClauses, fmt.Sprintf("o.stat_date between date %s and date %s", quoteSQLLiteral(period.From), quoteSQLLiteral(period.To)))
	}
	whereClauses = append(whereClauses, "o.final_price_local is not null")
	if strings.Contains(NormalizeText(text), "заверш") || strings.Contains(NormalizeText(text), "выполн") {
		whereClauses = append(whereClauses, "o.completed_orders = 1")
	}
	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "where " + strings.Join(whereClauses, " and ")
	}
	metricExpr := "round(avg(o.final_price_local)::numeric, 2)"

	return fmt.Sprintf(`
select
  o.stat_date as period_value,
  %s as metric_value
from analytics.v_incity_orders_latest o
%s
group by o.stat_date
having %s %s %s
order by period_value
limit 100
`, metricExpr, whereSQL, metricExpr, operator, strconv.FormatFloat(threshold, 'f', -1, 64)), true
}

func detectMetricThreshold(normalized string) (string, float64, bool) {
	operator := ""
	pattern := regexpMustCompile(`(?:выше|больше|дороже|превыш[а-я]*|>)\s+(\d+(?:[,.]\d+)?)`)
	if matches := pattern.FindStringSubmatch(normalized); len(matches) == 2 {
		operator = ">"
		value, err := strconv.ParseFloat(strings.ReplaceAll(matches[1], ",", "."), 64)
		return operator, value, err == nil
	}

	pattern = regexpMustCompile(`(?:ниже|меньше|дешевле|<)\s+(\d+(?:[,.]\d+)?)`)
	if matches := pattern.FindStringSubmatch(normalized); len(matches) == 2 {
		operator = "<"
		value, err := strconv.ParseFloat(strings.ReplaceAll(matches[1], ",", "."), 64)
		return operator, value, err == nil
	}
	return "", 0, false
}

func DetectCalendarMonthRange(text string) (TimeRange, bool) {
	normalized := NormalizeText(text)
	monthPattern := regexpMustCompile(`январ[а-я]*|феврал[а-я]*|март[а-я]*|апрел[а-я]*|ма[а-я]*|июн[а-я]*|июл[а-я]*|август[а-я]*|сентябр[а-я]*|октябр[а-я]*|ноябр[а-я]*|декабр[а-я]*`)
	monthText := monthPattern.FindString(normalized)
	if monthText == "" {
		return TimeRange{}, false
	}
	month, ok := russianMonthFromText(monthText)
	if !ok {
		return TimeRange{}, false
	}
	yearMatches := regexpMustCompile(`20\d{2}`).FindString(normalized)
	if yearMatches == "" {
		return TimeRange{}, false
	}
	year, err := strconv.Atoi(yearMatches)
	if err != nil {
		return TimeRange{}, false
	}
	from := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 1, -1)
	return TimeRange{
		Label: fmt.Sprintf("%s %d", russianMonthLabel(month), year),
		From:  from.Format("2006-01-02"),
		To:    to.Format("2006-01-02"),
		Grain: "day",
	}, true
}

// compactSQL нормализует граничные значения перед дальнейшим использованием.
func compactSQL(sqlText string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(sqlText)), " ")
}

// DetectOrderPriceThresholdShare recognizes "what percent of orders cost more than N".
func DetectOrderPriceThresholdShare(text string) (OrderPriceThresholdShare, bool) {
	normalized := NormalizeText(text)
	if normalized == "" {
		return OrderPriceThresholdShare{}, false
	}
	if !strings.Contains(normalized, "процент") && !strings.Contains(normalized, "доля") {
		return OrderPriceThresholdShare{}, false
	}
	if !strings.Contains(normalized, "заказ") {
		return OrderPriceThresholdShare{}, false
	}
	if !strings.Contains(normalized, "стоимост") &&
		!strings.Contains(normalized, "цен") &&
		!strings.Contains(normalized, "чек") {
		return OrderPriceThresholdShare{}, false
	}

	operator := ""
	if strings.Contains(normalized, "выше") ||
		strings.Contains(normalized, "больше") ||
		strings.Contains(normalized, "дороже") ||
		strings.Contains(normalized, "превыш") {
		operator = ">"
	}
	if strings.Contains(normalized, "ниже") ||
		strings.Contains(normalized, "меньше") ||
		strings.Contains(normalized, "дешевле") {
		operator = "<"
	}
	if operator == "" {
		return OrderPriceThresholdShare{}, false
	}

	threshold, ok := detectMoneyThreshold(normalized)
	if !ok {
		return OrderPriceThresholdShare{}, false
	}
	return OrderPriceThresholdShare{Threshold: threshold, Operator: operator}, true
}

func detectMoneyThreshold(text string) (float64, bool) {
	re := regexpMustCompile(`(\d+(?:[,.]\d+)?)\s*(?:руб|₽|р\b|р\s|рубл|р[уy]б)?`)
	matches := re.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		valueText := strings.ReplaceAll(match[1], ",", ".")
		value, err := strconv.ParseFloat(valueText, 64)
		if err == nil && value > 0 {
			return value, true
		}
	}
	return 0, false
}

func buildOrderPriceThresholdShareSQL(text string, intent Intent) (string, bool) {
	threshold, ok := DetectOrderPriceThresholdShare(text)
	if !ok {
		return "", false
	}

	priceExpr := "coalesce(io.price_order_local, io.price_tender_local, io.price_start_local, 0)"
	whereClauses := []string{"io.order_timestamp is not null"}
	if hasValidDateBounds(intent.Period.From, intent.Period.To) {
		whereClauses = append(whereClauses, fmt.Sprintf("io.order_timestamp::date between date %s and date %s", quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To)))
	}
	for _, filter := range intent.Filters {
		if filter.Operator != "=" {
			continue
		}
		switch filter.Field {
		case "city":
			whereClauses = append(whereClauses, fmt.Sprintf("io.city_id = %s", quoteSQLLiteral(filter.Value)))
		case "status_order":
			whereClauses = append(whereClauses, fmt.Sprintf("io.status_order = %s", quoteSQLLiteral(filter.Value)))
		case "status_tender":
			whereClauses = append(whereClauses, fmt.Sprintf("io.status_tender = %s", quoteSQLLiteral(filter.Value)))
		}
	}

	return fmt.Sprintf(`
select
	round(
		count(distinct case when %s %s %s then io.order_id end)::numeric
		/ nullif(count(distinct io.order_id), 0),
		4
	) as metric_value
from analytics.incity io
where %s
`,
		priceExpr,
		threshold.Operator,
		formatSQLNumber(threshold.Threshold),
		strings.Join(whereClauses, " and "),
	), true
}

func LooksLikeCheapestDailyTripsRequest(text string) bool {
	normalized := NormalizeText(text)
	if !hasAnyNormalized(normalized, "дешев", "минимальн") {
		return false
	}
	if !hasAnyNormalized(normalized, "поезд", "заказ") {
		return false
	}
	if !hasAnyNormalized(normalized, "дн", "день", "кажд") {
		return false
	}
	return regexpMustCompile(`(?:топ|top|покажи|найди)?\s*\d{1,3}\s+сам`).MatchString(normalized) ||
		regexpMustCompile(`(?:топ|top)\s*[- ]?\s*\d{1,3}`).MatchString(normalized)
}

func buildCheapestDailyTripsSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeCheapestDailyTripsRequest(text) {
		return "", false
	}

	limit := intent.Limit
	if limit <= 0 {
		limit = detectTopLimit(NormalizeText(text))
	}
	if limit <= 0 {
		limit = 10
	}
	limit = minInt(limit, 100)

	period, ok := detectMonthWithOptionalDayCount(text)
	if !ok {
		period = intent.Period
	}
	if !hasValidDateBounds(period.From, period.To) {
		return "", false
	}

	whereClauses := []string{
		fmt.Sprintf("o.stat_date between date %s and date %s", quoteSQLLiteral(period.From), quoteSQLLiteral(period.To)),
		"o.final_price_local is not null",
		"o.completed_orders = 1",
	}
	for _, filter := range intent.Filters {
		if canonicalFilterField(filter.Field) == "city" && filter.Operator == "=" {
			whereClauses = append(whereClauses, fmt.Sprintf("o.city = %s", quoteSQLLiteral(filter.Value)))
		}
	}

	return fmt.Sprintf(`
with daily_ranked as (
	select
		o.stat_date as period_value,
		o.city as group_value,
		o.final_price_local as metric_value,
		row_number() over (
			partition by o.stat_date
			order by o.final_price_local asc
		) as price_rank
	from analytics.v_incity_orders_latest o
	where %s
),
daily_cheapest as (
	select
		period_value,
		group_value,
		metric_value
	from daily_ranked
	where price_rank = 1
)
select
	period_value::text as period_value,
	metric_value,
	group_value
from daily_cheapest
order by metric_value asc, period_value asc
limit %d
`, strings.Join(whereClauses, " and "), limit), true
}

func detectTopLimit(normalized string) int {
	patterns := []*regexp.Regexp{
		regexpMustCompile(`(?:топ|top)\s*[- ]?\s*(\d{1,3})`),
		regexpMustCompile(`(?:покажи|найди)?\s*(\d{1,3})\s+сам`),
	}
	for _, pattern := range patterns {
		match := pattern.FindStringSubmatch(normalized)
		if len(match) != 2 {
			continue
		}
		value, err := strconv.Atoi(match[1])
		if err == nil && value > 0 {
			return value
		}
	}
	return 0
}

func detectMonthWithOptionalDayCount(text string) (TimeRange, bool) {
	normalized := NormalizeText(text)
	monthPattern := regexpMustCompile(`январ[а-я]*|феврал[а-я]*|март[а-я]*|апрел[а-я]*|ма[а-я]*|июн[а-я]*|июл[а-я]*|август[а-я]*|сентябр[а-я]*|октябр[а-я]*|ноябр[а-я]*|декабр[а-я]*`)
	monthText := monthPattern.FindString(normalized)
	if monthText == "" {
		return TimeRange{}, false
	}
	month, ok := russianMonthFromText(monthText)
	if !ok {
		return TimeRange{}, false
	}
	yearText := regexpMustCompile(`20\d{2}`).FindString(normalized)
	if yearText == "" {
		return TimeRange{}, false
	}
	year, err := strconv.Atoi(yearText)
	if err != nil {
		return TimeRange{}, false
	}

	from := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	monthEnd := from.AddDate(0, 1, -1)
	to := monthEnd
	if match := regexpMustCompile(`(\d{1,2})\s+дн`).FindStringSubmatch(normalized); len(match) == 2 {
		days, err := strconv.Atoi(match[1])
		if err == nil && days > 0 && days < monthEnd.Day() {
			to = from.AddDate(0, 0, days-1)
		}
	}
	return TimeRange{
		Label: fmt.Sprintf("%s %d", russianMonthLabel(month), year),
		From:  from.Format("2006-01-02"),
		To:    to.Format("2006-01-02"),
		Grain: "day",
	}, true
}

func formatSQLNumber(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// DetectMetricMovementConditions finds metric-direction pairs like "orders fell, cancellations grew".
func DetectMetricMovementConditions(text string) ([]MetricMovementCondition, bool) {
	normalized := NormalizeText(text)
	if normalized == "" {
		return nil, false
	}

	mentions := metricMovementMentions(normalized)
	directions := directionMentions(normalized)
	if len(mentions) == 0 || len(directions) == 0 {
		return nil, false
	}

	conditions := make([]MetricMovementCondition, 0, len(directions))
	seen := map[string]bool{}
	for _, direction := range directions {
		metric, ok := nearestMetricForDirection(mentions, direction)
		if !ok {
			continue
		}
		key := metric.metric + ":" + direction.direction
		if seen[key] {
			continue
		}
		conditions = append(conditions, MetricMovementCondition{
			Metric:    metric.metric,
			Direction: direction.direction,
		})
		seen[key] = true
	}
	return conditions, len(conditions) > 0
}

type movementMention struct {
	metric string
	start  int
	end    int
}

type movementDirection struct {
	direction string
	start     int
	end       int
}

func metricMovementMentions(text string) []movementMention {
	terms := []struct {
		metric string
		terms  []string
	}{
		{"cancellation_rate", []string{"соотношение отмен", "доля отмен", "процент отмен"}},
		{"completed_orders", []string{"завершенные заказы", "завершенные поездки", "выполненные заказы", "выполненные поездки", "заверш", "выполнен"}},
		{"cancellations", []string{"количество отмен", "отмененные заказы", "отмены", "отмен"}},
		{"avg_price", []string{"средняя стоимость", "средний чек", "средняя цена", "стоимость", "цена"}},
		{"avg_distance_meters", []string{"средняя дистанция", "среднее расстояние", "дистанц", "расстояни"}},
		{"avg_duration_minutes", []string{"средняя длительность", "среднее время", "длительност", "время поездки"}},
		{"revenue", []string{"выручка", "оборот", "доход", "деньги"}},
		{"total_orders", []string{"все заказы", "количество заказов", "число заказов", "заказы", "заказ"}},
	}

	mentions := make([]movementMention, 0)
	for _, item := range terms {
		for _, rawTerm := range item.terms {
			term := NormalizeText(rawTerm)
			if term == "" {
				continue
			}
			offset := 0
			for {
				index := strings.Index(text[offset:], term)
				if index < 0 {
					break
				}
				start := offset + index
				mentions = append(mentions, movementMention{
					metric: item.metric,
					start:  start,
					end:    start + len(term),
				})
				offset = start + len(term)
			}
		}
	}

	sort.SliceStable(mentions, func(i, j int) bool {
		if mentions[i].start == mentions[j].start {
			return mentions[i].end > mentions[j].end
		}
		return mentions[i].start < mentions[j].start
	})
	return mentions
}

func directionMentions(text string) []movementDirection {
	terms := []struct {
		direction string
		terms     []string
	}{
		{"down", []string{"просели", "просел", "просела", "просело", "снизились", "снизился", "снизилась", "снизило", "упали", "упал", "упала", "уменьшились", "уменьшился", "сократились", "сократился", "падение"}},
		{"up", []string{"выросли", "вырос", "выросла", "выросло", "увеличились", "увеличился", "увеличилась", "поднялись", "поднялся", "рост"}},
	}

	directions := make([]movementDirection, 0)
	for _, item := range terms {
		for _, rawTerm := range item.terms {
			term := NormalizeText(rawTerm)
			offset := 0
			for {
				index := strings.Index(text[offset:], term)
				if index < 0 {
					break
				}
				start := offset + index
				directions = append(directions, movementDirection{
					direction: item.direction,
					start:     start,
					end:       start + len(term),
				})
				offset = start + len(term)
			}
		}
	}

	sort.SliceStable(directions, func(i, j int) bool { return directions[i].start < directions[j].start })
	return directions
}

func nearestMetricForDirection(mentions []movementMention, direction movementDirection) (movementMention, bool) {
	best := movementMention{}
	bestDistance := 1 << 30
	for _, mention := range mentions {
		distance := bestDistance
		if mention.end <= direction.start {
			distance = direction.start - mention.end
			if distance > 80 {
				continue
			}
		} else if mention.start >= direction.end {
			distance = mention.start - direction.end
			if distance > 50 {
				continue
			}
		} else {
			distance = 0
		}
		if distance < bestDistance {
			best = mention
			bestDistance = distance
		}
	}
	return best, bestDistance != 1<<30
}

func buildMetricMovementFilterSQL(text string, intent Intent) (string, bool) {
	conditions := intent.MovementConditions
	if len(conditions) == 0 {
		if detected, ok := DetectMetricMovementConditions(text); ok {
			conditions = detected
		}
	}
	conditions = normalizeMovementConditions(conditions)
	if len(conditions) == 0 {
		return "", false
	}

	selectColumns := make([]string, 0, len(conditions)*2+1)
	selectColumns = append(selectColumns, "movement.stat_date::text as period_value")
	dailyColumns := make([]string, 0, len(conditions))
	movementColumns := make([]string, 0, len(conditions)*3)
	whereClauses := make([]string, 0, len(conditions)+1)
	for _, condition := range conditions {
		metricExpr, ok := metricExpressionWithAlias(condition.Metric, "vm")
		if !ok {
			return "", false
		}
		valueAlias := movementValueAlias(condition.Metric)
		deltaAlias := movementDeltaAlias(condition.Metric)
		dailyColumns = append(dailyColumns, fmt.Sprintf("coalesce(%s, 0) as %s", metricExpr, valueAlias))
		movementColumns = append(movementColumns,
			valueAlias,
			fmt.Sprintf("lag(%s) over (order by stat_date) as %s", valueAlias, movementPreviousAlias(condition.Metric)),
			fmt.Sprintf("%s - lag(%s) over (order by stat_date) as %s", valueAlias, valueAlias, deltaAlias),
		)
		selectColumns = append(selectColumns,
			fmt.Sprintf("movement.%s as %s", valueAlias, valueAlias),
			fmt.Sprintf("movement.%s as %s", deltaAlias, deltaAlias),
		)
		switch condition.Direction {
		case "up":
			whereClauses = append(whereClauses, fmt.Sprintf("movement.%s > 0", deltaAlias))
		case "down":
			whereClauses = append(whereClauses, fmt.Sprintf("movement.%s < 0", deltaAlias))
		default:
			return "", false
		}
	}

	joinConditions := []string{"vm.stat_date = series.period_value"}
	joinConditions = append(joinConditions, buildFilterClauses("vm", intent)...)

	if hasValidDateBounds(intent.Period.From, intent.Period.To) {
		whereClauses = append([]string{
			fmt.Sprintf("movement.stat_date between date %s and date %s", quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To)),
		}, whereClauses...)
		return fmt.Sprintf(`
with series as (
	select generate_series(
		date %s - interval '1 day',
		date %s,
		interval '1 day'
	)::date as period_value
),
daily as (
	select
		series.period_value as stat_date,
		%s
	from series
	left join analytics.v_ride_metrics vm on %s
	group by series.period_value
),
movement as (
	select
		stat_date,
		%s
	from daily
)
select
	%s
from movement
where %s
order by movement.stat_date asc
`, quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To), strings.Join(dailyColumns, ",\n\t\t"), strings.Join(joinConditions, " and "), strings.Join(movementColumns, ",\n\t\t"), strings.Join(selectColumns, ",\n\t"), strings.Join(whereClauses, " and ")), true
	}

	boundsWhere := buildFilterClauses("vm", intent)
	boundsWhereSQL := ""
	if len(boundsWhere) > 0 {
		boundsWhereSQL = " where " + strings.Join(boundsWhere, " and ")
	}
	whereClauses = append([]string{
		"movement.stat_date between (select from_date from bounds) and (select to_date from bounds)",
	}, whereClauses...)
	return fmt.Sprintf(`
with bounds as (
	select min(vm.stat_date)::date as from_date, max(vm.stat_date)::date as to_date
	from analytics.v_ride_metrics vm%s
),
series as (
	select generate_series(
		(select from_date - interval '1 day' from bounds),
		(select to_date from bounds),
		interval '1 day'
	)::date as period_value
),
daily as (
	select
		series.period_value as stat_date,
		%s
	from series
	left join analytics.v_ride_metrics vm on %s
	group by series.period_value
),
movement as (
	select
		stat_date,
		%s
	from daily
)
select
	%s
from movement
where %s
order by movement.stat_date asc
`, boundsWhereSQL, strings.Join(dailyColumns, ",\n\t\t"), strings.Join(joinConditions, " and "), strings.Join(movementColumns, ",\n\t\t"), strings.Join(selectColumns, ",\n\t"), strings.Join(whereClauses, " and ")), true
}

func normalizeMovementConditions(conditions []MetricMovementCondition) []MetricMovementCondition {
	normalized := make([]MetricMovementCondition, 0, len(conditions))
	seen := map[string]bool{}
	for _, condition := range conditions {
		metric := strings.TrimSpace(condition.Metric)
		direction := normalizeMovementDirection(condition.Direction)
		if metric == "" || direction == "" {
			continue
		}
		if _, ok := metricExpressionWithAlias(metric, "vm"); !ok {
			continue
		}
		key := metric + ":" + direction
		if seen[key] {
			continue
		}
		normalized = append(normalized, MetricMovementCondition{Metric: metric, Direction: direction})
		seen[key] = true
	}
	return normalized
}

func normalizeMovementDirection(direction string) string {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case "up", "increase", "increased", "growth", "grow":
		return "up"
	case "down", "decrease", "decreased", "drop", "fall", "fell":
		return "down"
	default:
		return ""
	}
}

func movementValueAlias(metric string) string {
	return safeMetricAlias(metric) + "_value"
}

func movementPreviousAlias(metric string) string {
	return safeMetricAlias(metric) + "_previous_value"
}

func movementDeltaAlias(metric string) string {
	return safeMetricAlias(metric) + "_delta"
}

func safeMetricAlias(metric string) string {
	normalized := strings.ToLower(strings.TrimSpace(metric))
	normalized = regexpMustCompile(`[^a-z0-9_]+`).ReplaceAllString(normalized, "_")
	normalized = strings.Trim(normalized, "_")
	if normalized == "" {
		return "metric"
	}
	return normalized
}

// LooksLikeRollingCityMovementComparison detects city filters such as:
// revenue grew in the last N days versus the previous N days, and cancellation rate grew too.
func LooksLikeRollingCityMovementComparison(text string) bool {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "последн") || !strings.Contains(normalized, "предыдущ") {
		return false
	}
	if !strings.Contains(normalized, "город") {
		return false
	}
	conditions, ok := DetectMetricMovementConditions(normalized)
	if !ok {
		return false
	}
	hasRevenueUp := false
	hasCancellationRateUp := false
	for _, condition := range normalizeMovementConditions(conditions) {
		if condition.Metric == "revenue" && condition.Direction == "up" {
			hasRevenueUp = true
		}
		if condition.Metric == "cancellation_rate" && condition.Direction == "up" {
			hasCancellationRateUp = true
		}
	}
	return hasRevenueUp && hasCancellationRateUp
}

func buildRollingCityMovementComparisonSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeRollingCityMovementComparison(text) && strings.TrimSpace(intent.GroupBy) != "city" {
		return "", false
	}
	if !LooksLikeRollingCityMovementComparison(text) {
		return "", false
	}

	normalized := NormalizeText(text)
	match := regexp.MustCompile(`(\d{1,3})\s+дн`).FindStringSubmatch(normalized)
	if len(match) != 2 {
		return "", false
	}
	days, err := strconv.Atoi(match[1])
	if err != nil || days <= 0 || days > 366 {
		return "", false
	}

	filterClauses := buildFilterClauses("vm", intent)
	filterSQL := ""
	if len(filterClauses) > 0 {
		filterSQL = " and " + strings.Join(filterClauses, " and ")
	}

	return fmt.Sprintf(`
with anchor as (
	select max(stat_date)::date as max_date
	from analytics.v_ride_metrics
),
city_windows as (
	select
		vm.city as group_value,
		coalesce(round(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[1]d days')::date and (select max_date from anchor) then vm.gross_revenue_local else 0 end)::numeric, 2), 0) as current_revenue,
		coalesce(round(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[2]d days')::date and ((select max_date from anchor) - interval '%[3]d days')::date then vm.gross_revenue_local else 0 end)::numeric, 2), 0) as previous_revenue,
		coalesce(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[1]d days')::date and (select max_date from anchor) then vm.cancelled_orders else 0 end)::numeric / nullif(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[1]d days')::date and (select max_date from anchor) then vm.total_orders else 0 end), 0), 0) as current_cancellation_rate,
		coalesce(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[2]d days')::date and ((select max_date from anchor) - interval '%[3]d days')::date then vm.cancelled_orders else 0 end)::numeric / nullif(sum(case when vm.stat_date between ((select max_date from anchor) - interval '%[2]d days')::date and ((select max_date from anchor) - interval '%[3]d days')::date then vm.total_orders else 0 end), 0), 0) as previous_cancellation_rate
	from analytics.v_ride_metrics vm
	where vm.stat_date between ((select max_date from anchor) - interval '%[2]d days')::date and (select max_date from anchor)
		and vm.city is not null%[4]s
	group by vm.city
),
movement as (
	select
		group_value,
		current_revenue,
		previous_revenue,
		round((current_revenue - previous_revenue)::numeric, 2) as revenue_delta,
		round(current_cancellation_rate, 4) as current_cancellation_rate,
		round(previous_cancellation_rate, 4) as previous_cancellation_rate,
		round((current_cancellation_rate - previous_cancellation_rate)::numeric, 4) as cancellation_rate_delta
	from city_windows
)
select
	group_value,
	current_revenue as metric_value,
	current_revenue,
	previous_revenue,
	revenue_delta,
	current_cancellation_rate,
	previous_cancellation_rate,
	cancellation_rate_delta
from movement
where revenue_delta > 0
	and cancellation_rate_delta > 0
order by revenue_delta desc, group_value asc
limit 100
`, days-1, days*2-1, days, filterSQL), true
}

func LooksLikeRollingMultiMetricComparison(text string) bool {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "последн") || !strings.Contains(normalized, "предыдущ") {
		return false
	}
	if !LooksLikeComparisonRequest(normalized) {
		return false
	}
	metricHits := 0
	if hasAnyNormalized(normalized, "выручк", "оборот", "доход") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "заверш", "выполнен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "отмен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "средн") && hasAnyNormalized(normalized, "стоимост", "цен", "чек") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "доля отмен", "процент отмен", "соотношение отмен") {
		metricHits++
	}
	return metricHits >= 3
}

func IsMultiMetricResultColumns(columns []string) bool {
	count := 0
	for _, column := range columns {
		switch strings.ToLower(strings.TrimSpace(column)) {
		case "revenue_value",
			"completed_orders_value",
			"cancellations_value",
			"avg_price_value",
			"cancellation_rate_value",
			"total_orders_value",
			"avg_distance_value",
			"avg_duration_value":
			count++
		}
	}
	return count >= 2
}

func buildRollingMultiMetricComparisonSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeRollingMultiMetricComparison(text) {
		return "", false
	}
	normalized := NormalizeText(text)
	match := regexp.MustCompile(`(\d{1,3})\s+дн`).FindStringSubmatch(normalized)
	if len(match) != 2 {
		return "", false
	}
	days, err := strconv.Atoi(match[1])
	if err != nil || days <= 0 || days > 366 {
		return "", false
	}

	joinConditions := []string{"vm.stat_date between p.from_date and p.to_date"}
	joinConditions = append(joinConditions, buildFilterClauses("vm", intent)...)

	return fmt.Sprintf(`
with anchor as (
	select max(stat_date)::date as max_date
	from analytics.v_ride_metrics
),
periods(period_order, period_value, from_date, to_date) as (
	values
		(1, %s, ((select max_date from anchor) - interval '%d days')::date, ((select max_date from anchor) - interval '%d days')::date),
		(2, %s, ((select max_date from anchor) - interval '%d days')::date, (select max_date from anchor))
)
select
	p.period_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as revenue_value,
	coalesce(sum(vm.completed_orders), 0) as completed_orders_value,
	coalesce(sum(vm.cancelled_orders), 0) as cancellations_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric / nullif(sum(vm.completed_orders), 0), 2), 0) as avg_price_value,
	coalesce(round(sum(vm.cancelled_orders)::numeric / nullif(sum(vm.total_orders), 0), 4), 0) as cancellation_rate_value
from periods p
left join analytics.v_ride_metrics vm on %s
group by p.period_order, p.period_value
order by p.period_order
limit 100
`,
		quoteSQLLiteral(fmt.Sprintf("предыдущие %d дней", days)),
		days*2-1,
		days,
		quoteSQLLiteral(fmt.Sprintf("последние %d дней", days)),
		days-1,
		strings.Join(joinConditions, " and "),
	), true
}

func LooksLikeCityMultiMetricComparison(text string) bool {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "город") {
		return false
	}
	if !LooksLikeComparisonRequest(normalized) && !strings.Contains(normalized, "по город") {
		return false
	}
	metricHits := 0
	if hasAnyNormalized(normalized, "выручк", "оборот", "доход") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "заверш", "выполнен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "отмен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "средн") && hasAnyNormalized(normalized, "стоимост", "цен", "чек") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "доля отмен", "процент отмен", "соотношение отмен") {
		metricHits++
	}
	return metricHits >= 3
}

func buildCityMultiMetricComparisonSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeCityMultiMetricComparison(text) {
		return "", false
	}

	whereClauses := buildWhereClauses("vm", intent, true)
	whereClauses = append(whereClauses, "vm.city is not null")
	whereSQL := "where " + strings.Join(whereClauses, " and ")

	return fmt.Sprintf(`
select
	vm.city as group_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as revenue_value,
	coalesce(sum(vm.completed_orders), 0) as completed_orders_value,
	coalesce(sum(vm.cancelled_orders), 0) as cancellations_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric / nullif(sum(vm.completed_orders), 0), 2), 0) as avg_price_value,
	coalesce(round(sum(vm.cancelled_orders)::numeric / nullif(sum(vm.total_orders), 0), 4), 0) as cancellation_rate_value
from analytics.v_ride_metrics vm
%s
group by vm.city
order by revenue_value desc, group_value asc
limit 100
`, whereSQL), true
}

func LooksLikeWeekdayBestWorstRequest(text string) bool {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "дн") || !strings.Contains(normalized, "недел") {
		return false
	}
	if !hasAnyNormalized(normalized, "лучш", "худш", "максим", "миним", "топ", "антитоп") {
		return false
	}
	metricHits := 0
	if hasAnyNormalized(normalized, "выручк", "оборот", "доход") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "заверш", "выполнен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "средн") && hasAnyNormalized(normalized, "стоимост", "цен", "чек") {
		metricHits++
	}
	return metricHits >= 2
}

func buildWeekdayBestWorstSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeWeekdayBestWorstRequest(text) {
		return "", false
	}
	normalized := NormalizeText(text)
	match := regexp.MustCompile(`(\d{1,3})\s+дн`).FindStringSubmatch(normalized)
	if len(match) != 2 {
		return "", false
	}
	days, err := strconv.Atoi(match[1])
	if err != nil || days <= 0 || days > 366 {
		return "", false
	}

	filterClauses := buildFilterClauses("vm", intent)
	filterSQL := ""
	if len(filterClauses) > 0 {
		filterSQL = " and " + strings.Join(filterClauses, " and ")
	}

	return fmt.Sprintf(`
with anchor as (
	select max(stat_date)::date as max_date
	from analytics.v_ride_metrics
),
weekday_metrics as (
	select
		date_part('isodow', vm.stat_date)::integer as weekday_number,
		case date_part('isodow', vm.stat_date)::integer
			when 1 then 'Понедельник'
			when 2 then 'Вторник'
			when 3 then 'Среда'
			when 4 then 'Четверг'
			when 5 then 'Пятница'
			when 6 then 'Суббота'
			when 7 then 'Воскресенье'
		end as weekday_value,
		coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as revenue_value,
		coalesce(sum(vm.completed_orders), 0) as completed_orders_value,
		coalesce(round(sum(vm.gross_revenue_local)::numeric / nullif(sum(vm.completed_orders), 0), 2), 0) as avg_price_value
	from analytics.v_ride_metrics vm
	where vm.stat_date between ((select max_date from anchor) - interval '%d days')::date and (select max_date from anchor)%s
	group by date_part('isodow', vm.stat_date)::integer
),
ranked as (
	select
		weekday_number,
		weekday_value,
		revenue_value,
		completed_orders_value,
		avg_price_value,
		dense_rank() over (order by revenue_value desc) as revenue_best_rank,
		dense_rank() over (order by revenue_value asc) as revenue_worst_rank,
		dense_rank() over (order by completed_orders_value desc) as completed_orders_best_rank,
		dense_rank() over (order by completed_orders_value asc) as completed_orders_worst_rank,
		dense_rank() over (order by avg_price_value desc) as avg_price_best_rank,
		dense_rank() over (order by avg_price_value asc) as avg_price_worst_rank
	from weekday_metrics
)
select
	weekday_value as group_value,
	revenue_value as metric_value,
	revenue_value,
	completed_orders_value,
	avg_price_value,
	revenue_best_rank,
	revenue_worst_rank,
	completed_orders_best_rank,
	completed_orders_worst_rank,
	avg_price_best_rank,
	avg_price_worst_rank
from ranked
order by weekday_number
limit 100
`, days-1, filterSQL), true
}

func LooksLikeLatestMonthHalfComparison(text string) bool {
	normalized := NormalizeText(text)
	if !LooksLikeComparisonRequest(normalized) {
		return false
	}
	if !strings.Contains(normalized, "половин") || !strings.Contains(normalized, "месяц") {
		return false
	}
	if !strings.Contains(normalized, "последн") && !strings.Contains(normalized, "доступн") {
		return false
	}
	metricHits := 0
	if hasAnyNormalized(normalized, "выручк", "оборот", "доход") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "заверш", "выполнен") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "средн") && hasAnyNormalized(normalized, "стоимост", "цен", "чек") {
		metricHits++
	}
	if hasAnyNormalized(normalized, "доля отмен", "процент отмен", "соотношение отмен") {
		metricHits++
	}
	return metricHits >= 2
}

func buildLatestMonthHalfComparisonSQL(text string, intent Intent) (string, bool) {
	if !LooksLikeLatestMonthHalfComparison(text) {
		return "", false
	}

	joinConditions := []string{"vm.stat_date between p.from_date and p.to_date"}
	joinConditions = append(joinConditions, buildFilterClauses("vm", intent)...)

	return fmt.Sprintf(`
with bounds as (
	select
		date_trunc('month', max(stat_date))::date as month_start,
		max(stat_date)::date as max_date
	from analytics.v_ride_metrics
),
periods(period_order, period_value, from_date, to_date) as (
	values
		(1, 'первая половина последнего доступного месяца', (select month_start from bounds), least(((select month_start from bounds) + interval '14 days')::date, (select max_date from bounds))),
		(2, 'вторая половина последнего доступного месяца', ((select month_start from bounds) + interval '15 days')::date, (select max_date from bounds))
)
select
	p.period_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as revenue_value,
	coalesce(sum(vm.completed_orders), 0) as completed_orders_value,
	coalesce(round(sum(vm.gross_revenue_local)::numeric / nullif(sum(vm.completed_orders), 0), 2), 0) as avg_price_value,
	coalesce(round(sum(vm.cancelled_orders)::numeric / nullif(sum(vm.total_orders), 0), 4), 0) as cancellation_rate_value
from periods p
left join analytics.v_ride_metrics vm on %s
where p.from_date <= p.to_date
group by p.period_order, p.period_value
order by p.period_order
limit 100
`, strings.Join(joinConditions, " and ")), true
}

func LooksLikeNamedWeekComparison(text string) bool {
	_, ok := detectNamedWeekComparison(text)
	return ok
}

func hasAnyNormalized(text string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(text, NormalizeText(needle)) {
			return true
		}
	}
	return false
}

func buildMonthlyWindowComparisonSQL(text string, intent Intent) (monthlyWindowComparison, string, bool) {
	comparison, ok := detectMonthlyWindowComparison(text)
	if !ok {
		return monthlyWindowComparison{}, "", false
	}

	metricExpr, ok := metricExpressionWithAlias(intent.Metric, "vm")
	if !ok {
		return monthlyWindowComparison{}, "", false
	}

	valueRows := make([]string, 0, len(comparison.Months))
	for index, month := range comparison.Months {
		from := time.Date(comparison.Year, month, 1, 0, 0, 0, 0, time.UTC)
		to := from.AddDate(0, 0, comparison.Days-1)
		label := fmt.Sprintf("Первые %d дней %s %d", comparison.Days, russianMonthLabel(month), comparison.Year)
		valueRows = append(valueRows, fmt.Sprintf(
			"(%s, %d, date %s, date %s)",
			quoteSQLLiteral(label),
			index+1,
			quoteSQLLiteral(from.Format("2006-01-02")),
			quoteSQLLiteral(to.Format("2006-01-02")),
		))
	}

	joinConditions := []string{"vm.stat_date between p.from_date and p.to_date"}
	joinConditions = append(joinConditions, buildFilterClauses("vm", intent)...)

	return comparison, fmt.Sprintf(`
with periods(period_value, period_order, from_date, to_date) as (
  values
    %s
)
select
  p.period_value,
  coalesce(%s, 0) as metric_value
from periods p
left join analytics.v_ride_metrics vm on %s
group by p.period_order, p.period_value
order by p.period_order
`,
		strings.Join(valueRows, ",\n    "),
		metricExpr,
		strings.Join(joinConditions, " and "),
	), true
}

// buildSpecialMultiMonthComparisonSQL строит производный результат из intent или данных сервиса.
func buildSpecialMultiMonthComparisonSQL(text string, intent Intent) (multiMonthComparison, string, bool) {
	comparison, ok := detectMultiMonthComparison(text)
	if !ok {
		return multiMonthComparison{}, "", false
	}

	metricExpr, ok := metricExpressionWithAlias(intent.Metric, "vm")
	if !ok {
		return multiMonthComparison{}, "", false
	}

	caseParts := make([]string, 0, len(comparison.Months))
	for _, month := range comparison.Months {
		label := fmt.Sprintf("%s %d", strings.Title(russianMonthLabel(month)), comparison.Year)
		caseParts = append(caseParts, fmt.Sprintf(
			"when vm.stat_date between date %s and date %s then %s",
			quoteSQLLiteral(time.Date(comparison.Year, month, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")),
			quoteSQLLiteral(time.Date(comparison.Year, month+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1).Format("2006-01-02")),
			quoteSQLLiteral(label),
		))
	}

	return comparison, fmt.Sprintf(`
select
  case
    %s
  end as period_value,
  %s as metric_value
from analytics.v_ride_metrics vm
where vm.stat_date between date %s and date %s
group by 1
order by min(vm.stat_date)
`,
		strings.Join(caseParts, " "),
		metricExpr,
		quoteSQLLiteral(comparison.From),
		quoteSQLLiteral(comparison.To),
	), true
}

// buildSpecialComparisonSQL строит производный результат из intent или данных сервиса.
func buildSpecialComparisonSQL(text string, intent Intent) (monthEdgeComparison, string, bool) {
	comparison, ok := detectNamedWeekComparison(text)
	if !ok {
		comparison, ok = detectMonthEdgeComparison(text)
	}
	if !ok {
		return monthEdgeComparison{}, "", false
	}

	metricExpr, ok := metricExpressionWithAlias(intent.Metric, "vm")
	if !ok {
		return monthEdgeComparison{}, "", false
	}

	return comparison, fmt.Sprintf(`
select
  case
    when vm.stat_date between date %s and date %s then %s
    when vm.stat_date between date %s and date %s then %s
  end as period_value,
  %s as metric_value
from analytics.v_ride_metrics vm
where vm.stat_date between date %s and date %s
   or vm.stat_date between date %s and date %s
group by 1
order by min(vm.stat_date)
`,
		quoteSQLLiteral(comparison.FirstFrom),
		quoteSQLLiteral(comparison.FirstTo),
		quoteSQLLiteral(comparison.FirstLabel),
		quoteSQLLiteral(comparison.LastFrom),
		quoteSQLLiteral(comparison.LastTo),
		quoteSQLLiteral(comparison.LastLabel),
		metricExpr,
		quoteSQLLiteral(comparison.FirstFrom),
		quoteSQLLiteral(comparison.FirstTo),
		quoteSQLLiteral(comparison.LastFrom),
		quoteSQLLiteral(comparison.LastTo),
	), true
}

func buildRollingPreviousPeriodComparisonSQL(text string, intent Intent) (string, bool) {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "последн") || !strings.Contains(normalized, "предыдущ") {
		return "", false
	}
	if !LooksLikeComparisonRequest(normalized) {
		return "", false
	}

	match := regexp.MustCompile(`(\d{1,3})\s+дн`).FindStringSubmatch(normalized)
	if len(match) != 2 {
		return "", false
	}
	days, err := strconv.Atoi(match[1])
	if err != nil || days <= 0 || days > 366 {
		return "", false
	}

	metric := intent.Metric
	if metric == "" && strings.Contains(normalized, "средн") &&
		(strings.Contains(normalized, "стоимост") || strings.Contains(normalized, "цен")) {
		metric = "avg_price"
	}
	metricExpr, ok := metricExpressionWithAlias(metric, "vm")
	if !ok {
		return "", false
	}

	return fmt.Sprintf(`
with anchor as (
  select max(stat_date)::date as max_date
  from analytics.v_ride_metrics
),
periods as (
  values
    (1, %s, ((select max_date from anchor) - interval '%d days')::date, ((select max_date from anchor) - interval '%d days')::date),
    (2, %s, ((select max_date from anchor) - interval '%d days')::date, (select max_date from anchor))
)
select
  p.column2 as period_value,
  coalesce(%s, 0) as metric_value
from periods p
left join analytics.v_ride_metrics vm on vm.stat_date between p.column3 and p.column4
group by p.column1, p.column2
order by p.column1
limit 100
`,
		quoteSQLLiteral(fmt.Sprintf("предыдущие %d дней", days)),
		days*2-1,
		days,
		quoteSQLLiteral(fmt.Sprintf("последние %d дней", days)),
		days-1,
		metricExpr,
	), true
}

// buildIntentSQL строит SQL из структурированного intent для legacy-тестов и совместимости.
func buildIntentSQL(text string, intent Intent) (string, error) {
	if strings.TrimSpace(intent.Metric) == "" {
		return "", fmt.Errorf("unable to build sql without a metric")
	}

	if intent.Limit <= 0 {
		if sqlText, ok := buildSpecialTimeSeriesSQL(text, intent); ok {
			return compactSQL(sqlText), nil
		}
	}

	source := metricSourceForIntent(text, intent)
	metricExpr, ok := metricExpressionForSource(intent.Metric, source)
	if !ok {
		return "", fmt.Errorf("unsupported metric %q", intent.Metric)
	}

	whereClauses := buildWhereClausesForSource(source, intent, true)
	groupExpr, groupAlias, isTimeGrouping, ok := groupExpressionForSource(intent.GroupBy, source)
	if !ok {
		return "", fmt.Errorf("unsupported group_by %q", intent.GroupBy)
	}

	var query strings.Builder
	if groupAlias == "" {
		fmt.Fprintf(&query, "select %s as metric_value from %s %s", metricExpr, source.Table, source.Alias)
	} else {
		fmt.Fprintf(&query, "select %s as %s, %s as metric_value from %s %s", groupExpr, groupAlias, metricExpr, source.Table, source.Alias)
	}

	if len(whereClauses) > 0 {
		query.WriteString(" where ")
		query.WriteString(strings.Join(whereClauses, " and "))
	}

	if groupAlias != "" {
		fmt.Fprintf(&query, " group by %s", groupExpr)
		if intent.Limit > 0 {
			fmt.Fprintf(&query, " order by metric_value desc, %s asc", groupExpr)
		} else if isTimeGrouping {
			fmt.Fprintf(&query, " order by %s asc", groupExpr)
		} else {
			fmt.Fprintf(&query, " order by metric_value desc, %s asc", groupExpr)
		}
		if intent.Limit > 0 {
			fmt.Fprintf(&query, " limit %d", minInt(intent.Limit, 100))
		}
	}

	return compactSQL(query.String()), nil
}

// buildSpecialTimeSeriesSQL строит производный результат из intent или данных сервиса.
func buildSpecialTimeSeriesSQL(text string, intent Intent) (string, bool) {
	if intent.GroupBy != "day" {
		return "", false
	}
	if intent.Period.From == "" || intent.Period.To == "" {
		return "", false
	}
	dayCount, ok := inclusiveDayCount(intent.Period.From, intent.Period.To)
	if !ok || dayCount <= 0 {
		return "", false
	}

	source := metricSourceForIntent(text, intent)
	metricExpr, ok := metricExpressionForSource(intent.Metric, source)
	if !ok {
		return "", false
	}

	joinConditions := []string{fmt.Sprintf("%s.%s = series.period_value", source.Alias, source.DateColumn)}
	joinConditions = append(joinConditions, buildFilterClausesForSource(source, intent)...)

	if !isRollingPeriodLabel(intent.Period.Label) {
		return fmt.Sprintf(`
with series as (
	select generate_series(
		date %s,
		date %s,
		interval '1 day'
	)::date as period_value
)
select
	series.period_value::text as period_value,
	coalesce(%s, 0) as metric_value
from series
left join %s %s on %s
group by series.period_value
order by series.period_value asc
`, quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To), metricExpr, source.Table, source.Alias, strings.Join(joinConditions, " and ")), true
	}

	return fmt.Sprintf(`
with anchor as (
	select max(%s)::date as max_date
	from %s
),
series as (
	select generate_series(
		(select max_date - interval '%d day' from anchor),
		(select max_date from anchor),
		interval '1 day'
	)::date as period_value
)
select
	series.period_value::text as period_value,
	coalesce(%s, 0) as metric_value
from series
left join %s %s on %s
group by series.period_value
order by series.period_value asc
`, source.DateColumn, source.Table, dayCount-1, metricExpr, source.Table, source.Alias, strings.Join(joinConditions, " and ")), true
}

// isRollingPeriodLabel отличает относительный период, который нужно привязать к данным.
func isRollingPeriodLabel(label string) bool {
	normalized := NormalizeText(label)
	return normalized == "" ||
		strings.HasPrefix(normalized, "послед") ||
		normalized == "сегодня" ||
		normalized == "вчера" ||
		normalized == "прошлая неделя" ||
		normalized == "прошлый месяц" ||
		normalized == "текущий месяц"
}

// metricExpressionWithAlias выполняет отдельный шаг окружающего сервисного сценария.
func rideMetricSource() metricSource {
	return metricSource{
		Table:      "analytics.v_ride_metrics",
		Alias:      "vm",
		DateColumn: "stat_date",
		CityColumn: "city",
		Kind:       "ride",
	}
}

func driverMetricSource() metricSource {
	return metricSource{
		Table:       "analytics.driver_detail",
		Alias:       "vm",
		DateColumn:  "tender_date_part",
		CityColumn:  "city_id",
		EntityID:    "driver_id",
		RegDate:     "driver_reg_date",
		Orders:      "orders",
		WithTenders: "orders_cnt_with_tenders",
		Accepted:    "orders_cnt_accepted",
		Completed:   "rides_count",
		OnlineTime:  "online_time_sum_seconds",
		RideTime:    "rides_time_sum_seconds",
		CancelAfter: "client_cancel_after_accept",
		Kind:        "driver",
	}
}

func passengerMetricSource() metricSource {
	return metricSource{
		Table:       "analytics.pass_detail",
		Alias:       "vm",
		DateColumn:  "order_date_part",
		CityColumn:  "city_id",
		EntityID:    "user_id",
		RegDate:     "user_reg_date",
		Orders:      "orders_count",
		WithTenders: "orders_cnt_with_tenders",
		Accepted:    "orders_cnt_accepted",
		Completed:   "rides_count",
		OnlineTime:  "online_time_sum_seconds",
		RideTime:    "rides_time_sum_seconds",
		CancelAfter: "client_cancel_after_accept",
		Kind:        "passenger",
	}
}

func metricSourceForIntent(text string, intent Intent) metricSource {
	intent = NormalizeEntityGroupBy(text, intent)
	metric := canonicalMetricID(intent.Metric)
	if intent.GroupBy == "driver" {
		return driverMetricSource()
	}
	if intent.GroupBy == "client" {
		return passengerMetricSource()
	}
	switch metric {
	case "active_drivers", "new_drivers":
		return driverMetricSource()
	case "active_passengers", "new_passengers":
		return passengerMetricSource()
	case "accepted_orders", "completed_rides", "acceptance_rate", "completion_rate", "cancel_after_accept_rate", "online_time_sum_seconds", "avg_online_time_seconds":
		if textMentionsPassenger(text) {
			return passengerMetricSource()
		}
		return driverMetricSource()
	default:
		return rideMetricSource()
	}
}

func ShouldBuildDomainMetricFromDetail(text string, intent Intent) bool {
	intent = NormalizeEntityGroupBy(text, intent)
	metric := canonicalMetricID(intent.Metric)
	if intent.GroupBy == "driver" || intent.GroupBy == "client" {
		return true
	}
	switch metric {
	case "active_drivers", "new_drivers", "active_passengers", "new_passengers":
		return true
	case "accepted_orders", "completed_rides", "acceptance_rate", "completion_rate", "cancel_after_accept_rate", "online_time_sum_seconds", "avg_online_time_seconds":
		return textMentionsDriver(text) || textMentionsPassenger(text)
	default:
		return false
	}
}

func NormalizeEntityGroupBy(text string, intent Intent) Intent {
	intent = NormalizeIntentAliases(intent)
	if intent.GroupBy != "" {
		return intent
	}
	if groupBy := entityBreakdownGroup(text); groupBy != "" {
		intent.GroupBy = groupBy
	}
	return intent
}

func entityBreakdownGroup(text string) string {
	normalized := NormalizeText(text)
	if normalized == "" {
		return ""
	}
	if strings.Contains(normalized, "по водител") ||
		strings.Contains(normalized, "разбивк") && strings.Contains(normalized, "водител") ||
		strings.Contains(normalized, "group by driver") ||
		strings.Contains(normalized, "by driver") {
		return "driver"
	}
	if strings.Contains(normalized, "по клиент") ||
		strings.Contains(normalized, "по пассажир") ||
		strings.Contains(normalized, "по пользовател") ||
		strings.Contains(normalized, "разбивк") && (strings.Contains(normalized, "клиент") || strings.Contains(normalized, "пассажир") || strings.Contains(normalized, "пользовател")) ||
		strings.Contains(normalized, "group by client") ||
		strings.Contains(normalized, "by client") ||
		strings.Contains(normalized, "by passenger") ||
		strings.Contains(normalized, "by user") {
		return "client"
	}
	return ""
}

func textMentionsDriver(text string) bool {
	normalized := NormalizeText(text)
	return strings.Contains(normalized, "водител") ||
		strings.Contains(normalized, "driver") ||
		strings.Contains(normalized, "supply")
}

func textMentionsPassenger(text string) bool {
	normalized := NormalizeText(text)
	return strings.Contains(normalized, "клиент") ||
		strings.Contains(normalized, "пассажир") ||
		strings.Contains(normalized, "пользовател") ||
		strings.Contains(normalized, "client") ||
		strings.Contains(normalized, "customer") ||
		strings.Contains(normalized, "passenger") ||
		strings.Contains(normalized, "demand")
}

func metricExpressionForSource(metric string, source metricSource) (string, bool) {
	metric = canonicalMetricID(metric)
	if source.Kind == "ride" {
		return metricExpressionWithAlias(metric, source.Alias)
	}

	column := func(name string) string {
		return source.Alias + "." + name
	}
	switch metric {
	case "total_orders":
		return fmt.Sprintf("sum(%s)", column(source.Orders)), true
	case "completed_orders":
		return fmt.Sprintf("sum(%s)", column(source.Completed)), true
	case "active_drivers":
		if source.Kind != "driver" {
			return "", false
		}
		return fmt.Sprintf("count(distinct %s)", column(source.EntityID)), true
	case "new_drivers":
		if source.Kind != "driver" {
			return "", false
		}
		return fmt.Sprintf("count(distinct case when %s = %s then %s end)", column(source.RegDate), column(source.DateColumn), column(source.EntityID)), true
	case "active_passengers":
		if source.Kind != "passenger" {
			return "", false
		}
		return fmt.Sprintf("count(distinct %s)", column(source.EntityID)), true
	case "new_passengers":
		if source.Kind != "passenger" {
			return "", false
		}
		return fmt.Sprintf("count(distinct case when %s = %s then %s end)", column(source.RegDate), column(source.DateColumn), column(source.EntityID)), true
	case "accepted_orders":
		return fmt.Sprintf("sum(%s)", column(source.Accepted)), true
	case "completed_rides":
		return fmt.Sprintf("sum(%s)", column(source.Completed)), true
	case "acceptance_rate":
		return fmt.Sprintf("round(sum(%s)::numeric / nullif(sum(%s), 0), 4)", column(source.Accepted), column(source.WithTenders)), true
	case "completion_rate":
		return fmt.Sprintf("round(sum(%s)::numeric / nullif(sum(%s), 0), 4)", column(source.Completed), column(source.Orders)), true
	case "cancel_after_accept_rate":
		return fmt.Sprintf("round(sum(%s)::numeric / nullif(sum(%s), 0), 4)", column(source.CancelAfter), column(source.Accepted)), true
	case "online_time_sum_seconds":
		return fmt.Sprintf("round(sum(%s)::numeric, 2)", column(source.OnlineTime)), true
	case "avg_online_time_seconds":
		return fmt.Sprintf("round(sum(%s)::numeric / nullif(count(distinct %s), 0), 2)", column(source.OnlineTime), column(source.EntityID)), true
	default:
		return "", false
	}
}

func groupExpressionForSource(groupBy string, source metricSource) (string, string, bool, bool) {
	column := func(name string) string {
		return source.Alias + "." + name
	}
	switch groupBy {
	case "":
		return "", "", false, true
	case "city":
		return column(source.CityColumn), "group_value", false, true
	case "day":
		return fmt.Sprintf("date_trunc('day', %s)::date", column(source.DateColumn)), "period_value", true, true
	case "week":
		return fmt.Sprintf("date_trunc('week', %s)::date", column(source.DateColumn)), "period_value", true, true
	case "month":
		return fmt.Sprintf("date_trunc('month', %s)::date", column(source.DateColumn)), "period_value", true, true
	case "driver":
		if source.Kind != "driver" {
			return "", "", false, false
		}
		return anonymizedEntityExpression(source, "driver"), "group_value", false, true
	case "client":
		if source.Kind != "passenger" {
			return "", "", false, false
		}
		return anonymizedEntityExpression(source, "client"), "group_value", false, true
	case "status_order", "status_tender":
		if source.Kind == "ride" {
			return groupExpressionWithAlias(groupBy, source.Alias)
		}
		return "", "", false, false
	default:
		return "", "", false, false
	}
}

func anonymizedEntityExpression(source metricSource, prefix string) string {
	return fmt.Sprintf("concat('%s_', substr(encode(digest(%s.%s, 'sha256'), 'hex'), 1, 12))", prefix, source.Alias, source.EntityID)
}

func buildWhereClausesForSource(source metricSource, intent Intent, includePeriod bool) []string {
	clauses := make([]string, 0, len(intent.Filters)+1)
	if includePeriod && hasValidDateBounds(intent.Period.From, intent.Period.To) {
		clauses = append(clauses, fmt.Sprintf("%s.%s between date %s and date %s", source.Alias, source.DateColumn, quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To)))
	}
	clauses = append(clauses, buildFilterClausesForSource(source, intent)...)
	return clauses
}

func buildFilterClausesForSource(source metricSource, intent Intent) []string {
	if source.Kind == "ride" {
		return buildFilterClauses(source.Alias, intent)
	}
	clauses := make([]string, 0, len(intent.Filters))
	for _, filter := range intent.Filters {
		if canonicalFilterField(filter.Field) != "city" || filter.Operator != "=" {
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s.%s = %s", source.Alias, source.CityColumn, quoteSQLLiteral(filter.Value)))
	}
	return clauses
}

func metricExpressionWithAlias(metric, alias string) (string, bool) {
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}

	switch metric {
	case "completed_orders":
		return fmt.Sprintf("sum(%s)", column("completed_orders")), true
	case "total_orders":
		return fmt.Sprintf("sum(%s)", column("total_orders")), true
	case "cancellations":
		return fmt.Sprintf("sum(%s)", column("cancelled_orders")), true
	case "cancellation_rate":
		return fmt.Sprintf("round(sum(%s)::numeric / nullif(sum(%s), 0), 4)", column("cancelled_orders"), column("completed_orders")), true
	case "revenue":
		return fmt.Sprintf("round(sum(%s)::numeric, 2)", column("gross_revenue_local")), true
	case "avg_price":
		return fmt.Sprintf("round(sum(%s) / nullif(sum(%s), 0), 2)", column("gross_revenue_local"), column("completed_orders")), true
	case "avg_distance_meters":
		return fmt.Sprintf("round(sum(%s * %s) / nullif(sum(%s), 0), 2)", column("avg_distance_meters"), column("completed_orders"), column("completed_orders")), true
	case "avg_duration_minutes":
		return fmt.Sprintf("round(sum(%s * %s) / nullif(sum(%s), 0) / 60.0, 2)", column("avg_duration_seconds"), column("completed_orders"), column("completed_orders")), true
	default:
		return "", false
	}
}

// groupExpressionWithAlias выполняет отдельный шаг окружающего сервисного сценария.
func groupExpressionWithAlias(groupBy, alias string) (string, string, bool, bool) {
	column := func(name string) string {
		if alias == "" {
			return name
		}
		return alias + "." + name
	}

	switch groupBy {
	case "":
		return "", "", false, true
	case "city":
		return column("city"), "group_value", false, true
	case "status_order":
		return column("status_order"), "group_value", false, true
	case "status_tender":
		return column("status_tender"), "group_value", false, true
	case "day":
		return fmt.Sprintf("date_trunc('day', %s)::date", column("stat_date")), "period_value", true, true
	case "week":
		return fmt.Sprintf("date_trunc('week', %s)::date", column("stat_date")), "period_value", true, true
	case "month":
		return fmt.Sprintf("date_trunc('month', %s)::date", column("stat_date")), "period_value", true, true
	default:
		return "", "", false, false
	}
}

// buildWhereClauses строит производный результат из intent или данных сервиса.
func buildWhereClauses(alias string, intent Intent, includePeriod bool) []string {
	clauses := make([]string, 0, len(intent.Filters)+2)
	if includePeriod && hasValidDateBounds(intent.Period.From, intent.Period.To) {
		column := "stat_date"
		if alias != "" {
			column = alias + "." + column
		}
		clauses = append(clauses, fmt.Sprintf("%s between date %s and date %s", column, quoteSQLLiteral(intent.Period.From), quoteSQLLiteral(intent.Period.To)))
	}

	clauses = append(clauses, buildFilterClauses(alias, intent)...)
	return clauses
}

// buildFilterClauses строит производный результат из intent или данных сервиса.
func buildFilterClauses(alias string, intent Intent) []string {
	clauses := make([]string, 0, len(intent.Filters))

	// В where-часть пропускаются только заранее разрешенные поля из semantic layer.
	allowedFields := map[string]string{
		"city":          "city",
		"status_order":  "status_order",
		"status_tender": "status_tender",
	}
	for _, filter := range intent.Filters {
		column, ok := allowedFields[filter.Field]
		if !ok || filter.Operator != "=" {
			continue
		}
		if alias != "" {
			column = alias + "." + column
		}
		clauses = append(clauses, fmt.Sprintf("%s = %s", column, quoteSQLLiteral(filter.Value)))
	}

	return clauses
}

// quoteSQLLiteral нормализует граничные значения перед дальнейшим использованием.
func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// inclusiveDayCount выполняет отдельный шаг окружающего сервисного сценария.
func inclusiveDayCount(from, to string) (int, bool) {
	fromDate, err := time.Parse("2006-01-02", from)
	if err != nil {
		return 0, false
	}
	toDate, err := time.Parse("2006-01-02", to)
	if err != nil {
		return 0, false
	}
	if toDate.Before(fromDate) {
		return 0, false
	}
	return int(toDate.Sub(fromDate).Hours()/24) + 1, true
}

// hasValidDateBounds проверяет, что обе границы периода являются ISO-датами.
func hasValidDateBounds(from, to string) bool {
	if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
		return false
	}
	_, fromErr := time.Parse("2006-01-02", strings.TrimSpace(from))
	_, toErr := time.Parse("2006-01-02", strings.TrimSpace(to))
	return fromErr == nil && toErr == nil
}

// detectMonthlyWindowComparison выводит аналитическое поведение из текста пользователя или результата.
func detectMonthlyWindowComparison(text string) (monthlyWindowComparison, bool) {
	normalized := NormalizeText(text)
	re := regexpMustCompile(`первые\s+(\d+)\s+дн[а-я]*\s+кажд[а-я]*\s+месяц[а-я]*.*(?:с|от)\s+([а-я]+)\s+(?:по|до)\s+([а-я]+)\s+(\d{4})`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) != 5 {
		return monthlyWindowComparison{}, false
	}

	days, err := strconv.Atoi(matches[1])
	if err != nil || days <= 0 {
		return monthlyWindowComparison{}, false
	}
	startMonth, ok := russianMonthFromText(matches[2])
	if !ok {
		return monthlyWindowComparison{}, false
	}
	endMonth, ok := russianMonthFromText(matches[3])
	if !ok || endMonth < startMonth {
		return monthlyWindowComparison{}, false
	}
	yearValue, err := strconv.Atoi(matches[4])
	if err != nil {
		return monthlyWindowComparison{}, false
	}

	months := make([]time.Month, 0, int(endMonth-startMonth)+1)
	for month := startMonth; month <= endMonth; month++ {
		firstDay := time.Date(yearValue, month, 1, 0, 0, 0, 0, time.UTC)
		lastDay := firstDay.AddDate(0, 1, -1)
		if days > lastDay.Day() {
			return monthlyWindowComparison{}, false
		}
		months = append(months, month)
	}

	from := time.Date(yearValue, startMonth, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(yearValue, endMonth, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days-1)
	return monthlyWindowComparison{
		Days:   days,
		Months: months,
		Year:   yearValue,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Label:  fmt.Sprintf("первые %d дней каждого месяца с %s по %s %d", days, russianMonthLabel(startMonth), russianMonthLabel(endMonth), yearValue),
	}, true
}

// detectMonthEdgeComparison выводит аналитическое поведение из текста пользователя или результата.
func detectMonthEdgeComparison(text string) (monthEdgeComparison, bool) {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "первые") || !strings.Contains(normalized, "последние") {
		return monthEdgeComparison{}, false
	}

	re := regexpMustCompile(`первые\s+(\d+)\s+дн[а-я]*\s+([а-я]+)\s+(\d{4}).*последние\s+(\d+)\s+дн[а-я]*\s+([а-я]+)\s+(\d{4})`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) != 7 {
		return monthEdgeComparison{}, false
	}

	firstDays, err := strconv.Atoi(matches[1])
	if err != nil || firstDays <= 0 {
		return monthEdgeComparison{}, false
	}
	lastDays, err := strconv.Atoi(matches[4])
	if err != nil || lastDays <= 0 {
		return monthEdgeComparison{}, false
	}
	firstMonthValue, ok := russianMonthFromText(strings.TrimSpace(matches[2]))
	if !ok {
		return monthEdgeComparison{}, false
	}
	firstYearValue, err := strconv.Atoi(matches[3])
	if err != nil {
		return monthEdgeComparison{}, false
	}
	lastMonthValue, ok := russianMonthFromText(strings.TrimSpace(matches[5]))
	if !ok {
		return monthEdgeComparison{}, false
	}
	lastYearValue, err := strconv.Atoi(matches[6])
	if err != nil {
		return monthEdgeComparison{}, false
	}

	firstStart := time.Date(firstYearValue, firstMonthValue, 1, 0, 0, 0, 0, time.UTC)
	firstLastDay := firstStart.AddDate(0, 1, -1)
	if firstDays > firstLastDay.Day() {
		return monthEdgeComparison{}, false
	}

	lastStartOfMonth := time.Date(lastYearValue, lastMonthValue, 1, 0, 0, 0, 0, time.UTC)
	lastMonthLastDay := lastStartOfMonth.AddDate(0, 1, -1)
	if lastDays > lastMonthLastDay.Day() {
		return monthEdgeComparison{}, false
	}

	firstLabel := russianMonthLabel(firstMonthValue)
	lastLabel := russianMonthLabel(lastMonthValue)
	return monthEdgeComparison{
		FirstDays:   firstDays,
		LastDays:    lastDays,
		Year:        firstYearValue,
		Month:       firstMonthValue,
		FirstFrom:   firstStart.Format("2006-01-02"),
		FirstTo:     firstStart.AddDate(0, 0, firstDays-1).Format("2006-01-02"),
		FirstLabel:  fmt.Sprintf("Первые %d дней %s %d", firstDays, firstLabel, firstYearValue),
		LastFrom:    lastMonthLastDay.AddDate(0, 0, -(lastDays - 1)).Format("2006-01-02"),
		LastTo:      lastMonthLastDay.Format("2006-01-02"),
		LastLabel:   fmt.Sprintf("Последние %d дней %s %d", lastDays, lastLabel, lastYearValue),
		PeriodLabel: fmt.Sprintf("сравнение первых %d дней %s %d и последних %d дней %s %d", firstDays, firstLabel, firstYearValue, lastDays, lastLabel, lastYearValue),
	}, true
}

// detectNamedWeekComparison распознаёт сравнение последней и первой недели месяцев.
func detectNamedWeekComparison(text string) (monthEdgeComparison, bool) {
	normalized := NormalizeText(text)
	re := regexpMustCompile(`последн[а-я]*\s+недел[а-я]*\s+([а-я]+)\s+(\d{4}).*перв[а-я]*\s+недел[а-я]*\s+([а-я]+)\s+(\d{4})`)
	matches := re.FindStringSubmatch(normalized)
	if len(matches) != 5 {
		return monthEdgeComparison{}, false
	}
	firstMonth, ok := russianMonthFromText(matches[1])
	if !ok {
		return monthEdgeComparison{}, false
	}
	firstYear, err := strconv.Atoi(matches[2])
	if err != nil {
		return monthEdgeComparison{}, false
	}
	secondMonth, ok := russianMonthFromText(matches[3])
	if !ok {
		return monthEdgeComparison{}, false
	}
	secondYear, err := strconv.Atoi(matches[4])
	if err != nil {
		return monthEdgeComparison{}, false
	}

	firstTo := time.Date(firstYear, firstMonth+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	firstFrom := firstTo.AddDate(0, 0, -6)
	secondFrom := time.Date(secondYear, secondMonth, 1, 0, 0, 0, 0, time.UTC)
	secondTo := secondFrom.AddDate(0, 0, 6)
	firstLabel := fmt.Sprintf("Последняя неделя %s %d", russianMonthLabel(firstMonth), firstYear)
	secondLabel := fmt.Sprintf("Первая неделя %s %d", russianMonthLabel(secondMonth), secondYear)
	return monthEdgeComparison{
		FirstDays:   7,
		LastDays:    7,
		FirstFrom:   firstFrom.Format("2006-01-02"),
		FirstTo:     firstTo.Format("2006-01-02"),
		FirstLabel:  firstLabel,
		LastFrom:    secondFrom.Format("2006-01-02"),
		LastTo:      secondTo.Format("2006-01-02"),
		LastLabel:   secondLabel,
		PeriodLabel: fmt.Sprintf("%s и %s", firstLabel, secondLabel),
	}, true
}

// detectMultiMonthComparison выводит аналитическое поведение из текста пользователя или результата.
func detectMultiMonthComparison(text string) (multiMonthComparison, bool) {
	normalized := NormalizeText(text)
	if !strings.Contains(normalized, "сравн") {
		return multiMonthComparison{}, false
	}

	yearRe := regexpMustCompile(`(\d{4})`)
	yearMatches := yearRe.FindStringSubmatch(normalized)
	if len(yearMatches) != 2 {
		return multiMonthComparison{}, false
	}
	yearValue, err := strconv.Atoi(yearMatches[1])
	if err != nil {
		return multiMonthComparison{}, false
	}

	monthPattern := regexpMustCompile(`январ[а-я]*|феврал[а-я]*|март[а-я]*|апрел[а-я]*|ма[а-я]*|июн[а-я]*|июл[а-я]*|август[а-я]*|сентябр[а-я]*|октябр[а-я]*|ноябр[а-я]*|декабр[а-я]*`)
	rawMonths := monthPattern.FindAllString(normalized, -1)
	months := make([]time.Month, 0, len(rawMonths))
	seen := map[time.Month]bool{}
	for _, rawMonth := range rawMonths {
		monthValue, ok := russianMonthFromText(strings.TrimSpace(rawMonth))
		if !ok || seen[monthValue] {
			continue
		}
		months = append(months, monthValue)
		seen[monthValue] = true
	}
	if len(months) < 2 {
		return multiMonthComparison{}, false
	}

	sort.Slice(months, func(i, j int) bool { return months[i] < months[j] })
	from := time.Date(yearValue, months[0], 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(yearValue, months[len(months)-1]+1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	labels := make([]string, 0, len(months))
	for _, month := range months {
		labels = append(labels, fmt.Sprintf("%s %d", russianMonthLabel(month), yearValue))
	}

	return multiMonthComparison{
		Months: months,
		Year:   yearValue,
		From:   from.Format("2006-01-02"),
		To:     to.Format("2006-01-02"),
		Label:  strings.Join(labels, ", "),
	}, true
}

// russianMonthFromText выполняет отдельный шаг окружающего сервисного сценария.
func russianMonthFromText(value string) (time.Month, bool) {
	months := []struct {
		prefix string
		month  time.Month
	}{
		{"январ", time.January},
		{"феврал", time.February},
		{"март", time.March},
		{"апрел", time.April},
		{"май", time.May},
		{"мая", time.May},
		{"июн", time.June},
		{"июл", time.July},
		{"август", time.August},
		{"сентябр", time.September},
		{"октябр", time.October},
		{"ноябр", time.November},
		{"декабр", time.December},
	}
	normalized := NormalizeText(value)
	for _, item := range months {
		if strings.HasPrefix(normalized, item.prefix) {
			return item.month, true
		}
	}
	return 0, false
}

// russianMonthLabel выполняет отдельный шаг окружающего сервисного сценария.
func russianMonthLabel(month time.Month) string {
	labels := map[time.Month]string{
		time.January:   "января",
		time.February:  "февраля",
		time.March:     "марта",
		time.April:     "апреля",
		time.May:       "мая",
		time.June:      "июня",
		time.July:      "июля",
		time.August:    "августа",
		time.September: "сентября",
		time.October:   "октября",
		time.November:  "ноября",
		time.December:  "декабря",
	}
	return labels[month]
}

// minInt выполняет отдельный шаг окружающего сервисного сценария.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// regexpMustCompile выполняет отдельный шаг окружающего сервисного сценария.
func regexpMustCompile(pattern string) *regexp.Regexp {
	return regexp.MustCompile(pattern)
}
