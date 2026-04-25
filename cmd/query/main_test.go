package main

import (
	"strings"
	"testing"

	"drivee-self-service/internal/shared"
)

func TestChooseChartTreatsDateSeriesAsTimeSeriesEvenWithoutIntentGroupBy(t *testing.T) {
	intent := shared.Intent{Metric: "revenue"}
	result := shared.QueryResult{
		Columns: []string{"period_value", "metric_value"},
		Rows: [][]string{
			{"2025-03-01", "100"},
			{"2025-03-02", "110"},
			{"2025-03-03", "120"},
			{"2025-03-04", "130"},
			{"2025-03-05", "140"},
			{"2025-03-06", "150"},
			{"2025-03-07", "160"},
			{"2025-03-08", "170"},
			{"2025-03-09", "180"},
		},
		Count: 9,
	}

	spec := chooseChart(intent, result)
	if spec.Type != "area-line" {
		t.Fatalf("expected area-line, got %q", spec.Type)
	}
}

func TestChooseChartUsesBarForComparisonPattern(t *testing.T) {
	intent := shared.Intent{
		Metric:  "avg_price",
		Pattern: "monthly_window_comparison",
	}
	result := shared.QueryResult{
		Columns: []string{"period_label", "metric_value"},
		Rows: [][]string{
			{"Январь", "120"},
			{"Февраль", "130"},
			{"Март", "125"},
		},
		Count: 3,
	}

	spec := chooseChart(intent, result)
	if spec.Type != "bar" {
		t.Fatalf("expected bar, got %q", spec.Type)
	}
}

func TestChooseChartUsesTableForMultiMetricResult(t *testing.T) {
	intent := shared.Intent{Metric: "revenue", Pattern: "comparison"}
	result := shared.QueryResult{
		Columns: []string{"period_value", "metric_value", "revenue_value", "completed_orders_value", "cancellations_value", "avg_price_value", "cancellation_rate_value"},
		Rows: [][]string{
			{"предыдущие 30 дней", "1000", "1000", "10", "2", "100", "0.1"},
			{"последние 30 дней", "1200", "1200", "12", "3", "100", "0.12"},
		},
		Count: 2,
	}

	spec := chooseChart(intent, result)
	if spec.Type != "table" {
		t.Fatalf("expected table, got %q", spec.Type)
	}
}

func TestChooseChartDoesNotTreatGroupValueDatesAsTimeSeries(t *testing.T) {
	intent := shared.Intent{Metric: "revenue", GroupBy: "city"}
	result := shared.QueryResult{
		Columns: []string{"group_value", "metric_value"},
		Rows: [][]string{
			{"2025-03-01", "100"},
			{"2025-03-02", "110"},
			{"2025-03-03", "120"},
		},
		Count: 3,
	}

	spec := chooseChart(intent, result)
	if spec.Type != "bar" {
		t.Fatalf("expected bar, got %q", spec.Type)
	}
}

func TestChooseChartUsesHistogramForBucketResult(t *testing.T) {
	intent := shared.Intent{Metric: "avg_price", Pattern: "distribution"}
	result := shared.QueryResult{
		Columns: []string{"bucket_value", "metric_value"},
		Rows: [][]string{
			{"0 - 100", "12"},
			{"100 - 200", "24"},
		},
		Count: 2,
	}

	spec := chooseChart(intent, result)
	if spec.Type != "histogram" {
		t.Fatalf("expected histogram, got %q", spec.Type)
	}
}

func TestShouldAnchorPeriodToDataForRollingDemoRequest(t *testing.T) {
	period := shared.TimeRange{
		Label: "последние 30 дней",
		From:  "2026-03-25",
		To:    "2026-04-23",
		Grain: "day",
	}

	if !shouldAnchorPeriodToData("Покажи выручку по городам за последние 30 дней", period) {
		t.Fatal("expected rolling period to be anchored to data")
	}
}

func TestShouldAnchorPeriodToDataKeepsExplicitYear(t *testing.T) {
	period := shared.TimeRange{
		Label: "апреля 2025",
		From:  "2025-04-01",
		To:    "2025-04-30",
		Grain: "day",
	}

	if shouldAnchorPeriodToData("Покажи выручку по городам за апрель 2025", period) {
		t.Fatal("did not expect explicit period to be anchored to data")
	}
}

func TestShouldBuildSQLFromIntentBlocksLowConfidenceClarification(t *testing.T) {
	intent := shared.Intent{
		Metric:        "avg_price",
		Confidence:    0.58,
		Clarification: "Уточните, какую стоимость нужно посчитать.",
	}

	if shouldBuildSQLFromIntent(intent) {
		t.Fatal("expected low-confidence intent with clarification to be blocked")
	}
}

func TestShouldBuildSQLFromIntentBlocksUnsupportedComparison(t *testing.T) {
	intent := shared.Intent{
		Pattern:    "unsupported_comparison",
		Metric:     "revenue",
		Confidence: 0.62,
	}

	if shouldBuildSQLFromIntent(intent) {
		t.Fatal("expected unsupported comparison to be blocked")
	}
}

func TestShouldBuildSQLFromIntentAllowsSupportedPattern(t *testing.T) {
	intent := shared.Intent{
		Pattern:    "metric_timeseries",
		Metric:     "revenue",
		GroupBy:    "day",
		Confidence: 0.86,
	}

	if !shouldBuildSQLFromIntent(intent) {
		t.Fatal("expected supported high-confidence intent to be executable")
	}
}

func TestPreferBuilderSQLWhenModelSQLMissesMetricAlias(t *testing.T) {
	intent := shared.Intent{
		Pattern:    "metric_timeseries",
		Metric:     "revenue",
		GroupBy:    "day",
		Period:     shared.TimeRange{From: "2025-03-01", To: "2025-03-03", Grain: "day"},
		Confidence: 0.91,
	}
	modelSQL := "select stat_date, round(sum(gross_revenue_local)::numeric, 2) from analytics.v_ride_metrics group by stat_date"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи график выручки за 3 дня", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected SQL to be rebuilt from intent")
	}
	if !strings.Contains(sqlText, "period_value") || !strings.Contains(sqlText, "metric_value") {
		t.Fatalf("rebuilt sql = %q, want period_value and metric_value aliases", sqlText)
	}
}

func TestPreferBuilderSQLKeepsCompatibleModelSQL(t *testing.T) {
	intent := shared.Intent{
		Pattern:    "metric_timeseries",
		Metric:     "revenue",
		GroupBy:    "day",
		Confidence: 0.91,
	}
	modelSQL := "select stat_date as period_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by stat_date"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи график выручки", intent, modelSQL)
	if rebuilt {
		t.Fatal("did not expect compatible SQL to be rebuilt")
	}
	if sqlText != modelSQL {
		t.Fatalf("sqlText = %q, want original model SQL", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsConcatenatedPeriodValue(t *testing.T) {
	intent := shared.Intent{
		Pattern:    "metric_timeseries",
		Metric:     "revenue",
		GroupBy:    "day",
		Period:     shared.TimeRange{From: "2025-03-01", To: "2025-03-03", Grain: "day"},
		Confidence: 0.91,
	}
	modelSQL := "select concat(stat_date, ': ', round(sum(gross_revenue_local)::numeric, 2)) as period_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by stat_date"

	_, rebuilt := preferBuilderSQLForIntent("Покажи график выручки за 3 дня", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected concatenated period/value SQL to be rebuilt")
	}
}

func TestNormalizeImplicitPeriodClearsModelDefaultPeriod(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Period:     shared.TimeRange{Label: "последние 7 дней", From: "2026-04-18", To: "2026-04-24", Grain: "day"},
		Confidence: 0.9,
	}

	normalized := normalizeImplicitPeriod("Покажи выручку по городам", intent)
	if normalized.Period.Label != "весь доступный период" || normalized.Period.From != "" || normalized.Period.To != "" {
		t.Fatalf("period = %+v, want whole available period without bounds", normalized.Period)
	}
}

func TestPreferBuilderSQLRebuildsImplicitLastSevenDays(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Period:     shared.TimeRange{Label: "весь доступный период"},
		Confidence: 0.9,
	}
	modelSQL := "with bounds as (select max(stat_date) as max_date from analytics.v_ride_metrics) select city as group_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics where stat_date between ((select max_date from bounds) - interval '6 days')::date and (select max_date from bounds) group by city"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи выручку по городам", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected implicit last-7-days SQL to be rebuilt")
	}
	if strings.Contains(strings.ToLower(sqlText), "stat_date between") || strings.Contains(strings.ToLower(sqlText), "max(stat_date)") {
		t.Fatalf("rebuilt sql = %q, did not expect date restriction", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsBrokenCalendarMonthMaxDateSQL(t *testing.T) {
	intent := shared.Intent{
		Metric:     "avg_price",
		GroupBy:    "day",
		Period:     shared.TimeRange{Label: "ноября 2025", From: "2025-11-01", To: "2025-11-30", Grain: "day"},
		Confidence: 0.91,
	}
	modelSQL := "with bounds as (select max(stat_date) as max_date from analytics.v_ride_metrics) select stat_date as period_value, round(sum(gross_revenue_local)::numeric / nullif(sum(completed_orders), 0), 2) as metric_value from analytics.v_ride_metrics where stat_date between date '2025-11-01' and (select max_date from bounds where max_date < date '2025-12-01') and city is not null and status_order is not null and status_tender is not null group by stat_date having round(sum(gross_revenue_local)::numeric / nullif(sum(completed_orders), 0), 2) > 300 order by stat_date limit 100"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи дни, где средняя цена заказа больше 300 рублей за ноябрь 2025", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected broken calendar-month SQL to be rebuilt")
	}
	lower := strings.ToLower(sqlText)
	if strings.Contains(lower, "max_date") ||
		strings.Contains(lower, "city is not null") ||
		strings.Contains(lower, "status_order is not null") ||
		strings.Contains(lower, "status_tender is not null") {
		t.Fatalf("rebuilt sql = %q, did not expect max_date or unsolicited dimension null filters", sqlText)
	}
	if !strings.Contains(sqlText, "date '2025-11-30'") || !strings.Contains(lower, "having") || !strings.Contains(sqlText, "> 300") {
		t.Fatalf("rebuilt sql = %q, want November bounds and having > 300", sqlText)
	}
}

func TestPreferBuilderSQLUsesDistributionBuilder(t *testing.T) {
	intent := shared.Intent{
		Metric:     "avg_price",
		GroupBy:    "day",
		Confidence: 0.91,
	}
	modelSQL := "select stat_date as period_value, round(avg(avg_price_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by stat_date"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи гистограмму стоимости заказов", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected distribution request to be rebuilt")
	}
	if !strings.Contains(sqlText, "bucket_value") || !strings.Contains(sqlText, "width_bucket") {
		t.Fatalf("sqlText = %q, want histogram bucket SQL", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsCheapestDailyTrips(t *testing.T) {
	intent := shared.Intent{
		Metric:     "avg_price",
		Limit:      6,
		Confidence: 0.91,
	}
	modelSQL := "select final_price_local as metric_value from analytics.v_incity_orders_latest where stat_date between date '2026-03-01' and date '2026-03-31' and completed_orders = 1 order by final_price_local asc limit 6"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи 6 самых дешевых отдельных поездок за 30 дней марта 2026", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected cheapest daily trips request to be rebuilt")
	}
	if !strings.Contains(sqlText, "partition by o.stat_date") ||
		!strings.Contains(sqlText, "date '2026-03-30'") ||
		strings.Contains(sqlText, "date '2026-03-31'") {
		t.Fatalf("sqlText = %q, want daily cheapest query for first 30 days of March", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsDriverMetricFromDetail(t *testing.T) {
	intent := shared.Intent{
		Metric:     "active_drivers",
		GroupBy:    "city",
		Confidence: 0.91,
	}
	modelSQL := "select city as group_value, sum(active_drivers)::integer as metric_value from analytics.v_driver_daily_metrics group by city limit 100"

	sqlText, rebuilt := preferBuilderSQLForIntent("Сколько активных водителей по городам", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected driver metric SQL to be rebuilt from detail table")
	}
	if !strings.Contains(sqlText, "from analytics.driver_detail vm") || !strings.Contains(sqlText, "count(distinct vm.driver_id)") {
		t.Fatalf("sqlText = %q, want driver_detail distinct driver count", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsDriverBreakdownFromText(t *testing.T) {
	intent := shared.Intent{
		Metric:     "completed_rides",
		Filters:    []shared.Filter{{Field: "city", Operator: "=", Value: "60"}},
		Confidence: 0.91,
	}
	modelSQL := "select sum(completed_rides)::integer as metric_value from analytics.v_driver_daily_metrics where city = '60'"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи статистику завершенных поездок с разбивкой по водителям в городе 60", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected driver breakdown SQL to be rebuilt from text")
	}
	if !strings.Contains(sqlText, "from analytics.driver_detail vm") ||
		!strings.Contains(sqlText, "concat('driver_'") ||
		!strings.Contains(sqlText, "sum(vm.rides_count)") ||
		!strings.Contains(sqlText, "vm.city_id = '60'") {
		t.Fatalf("sqlText = %q, want anonymized driver breakdown from detail table", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsHistogramComparisonBuckets(t *testing.T) {
	intent := shared.Intent{
		Metric:     "avg_price",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "with stats as (select min(o.final_price_local)::numeric as min_value, max(o.final_price_local)::numeric as max_value from analytics.v_incity_orders_latest o where o.final_price_local is not null) select '80.81 - 605.47' as bucket_value, count(*)::integer as metric_value from stats"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи гистограммой сравнение средней стоимости поездки за последние 14 дней в данных и предыдущие 14 дней до них.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected bucket SQL to be rebuilt as period comparison")
	}
	if strings.Contains(sqlText, "bucket_value") || strings.Contains(sqlText, "width_bucket") {
		t.Fatalf("sqlText = %q, did not expect distribution buckets", sqlText)
	}
	if !strings.Contains(sqlText, "period_value") || !strings.Contains(sqlText, "последние 14 дней") || !strings.Contains(sqlText, "предыдущие 14 дней") {
		t.Fatalf("sqlText = %q, want rolling period comparison", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsRollingCityMovementComparison(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select city as group_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by city limit 100"

	sqlText, rebuilt := preferBuilderSQLForIntent("Покажи города, где за последние 30 дней в данных выручка выросла относительно предыдущих 30 дней, но доля отмен тоже выросла.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected rolling city movement comparison to be rebuilt")
	}
	if !strings.Contains(sqlText, "revenue_delta > 0") ||
		!strings.Contains(sqlText, "cancellation_rate_delta > 0") ||
		!strings.Contains(sqlText, "current_cancellation_rate") ||
		!strings.Contains(sqlText, "previous_cancellation_rate") {
		t.Fatalf("sqlText = %q, want revenue and cancellation-rate movement filters", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsRollingMultiMetricComparison(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select 'последние 30 дней' as period_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics limit 100"

	sqlText, rebuilt := preferBuilderSQLForIntent("Сравни последние 30 дней в данных с предыдущими 30 днями по выручке, завершённым поездкам, отменам, средней стоимости и доле отмен.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected rolling multi-metric comparison to be rebuilt")
	}
	for _, needle := range []string{
		"revenue_value",
		"completed_orders_value",
		"cancellations_value",
		"avg_price_value",
		"cancellation_rate_value",
		"предыдущие 30 дней",
		"последние 30 дней",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("sqlText = %q, want %q", sqlText, needle)
		}
	}
}

func TestPreferBuilderSQLRebuildsCityMultiMetricComparison(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select city as group_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by city"

	sqlText, rebuilt := preferBuilderSQLForIntent("Сравни города по выручке, завершённым поездкам, отменам, средней стоимости и доле отмен.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected city multi-metric comparison to be rebuilt")
	}
	for _, needle := range []string{"revenue_value", "completed_orders_value", "cancellations_value", "avg_price_value", "cancellation_rate_value"} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("sqlText = %q, want %q", sqlText, needle)
		}
	}
}

func TestPreferBuilderSQLRebuildsWeekdayBestWorst(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		GroupBy:    "day",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select d.weekday as group_value, sum(vm.gross_revenue_local) as metric_value from analytics.calendar d join analytics.v_ride_metrics vm on vm.stat_date = d.date group by d.weekday"

	sqlText, rebuilt := preferBuilderSQLForIntent("Определи лучшие и худшие дни недели по выручке, количеству завершённых поездок и средней стоимости за последние 90 дней в данных.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected weekday best/worst request to be rebuilt")
	}
	for _, needle := range []string{
		"analytics.v_ride_metrics",
		"date_part('isodow', vm.stat_date)",
		"revenue_best_rank",
		"completed_orders_worst_rank",
		"avg_price_best_rank",
		"interval '89 days'",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("sqlText = %q, want %q", sqlText, needle)
		}
	}
	if strings.Contains(sqlText, "analytics.calendar") {
		t.Fatalf("sqlText = %q, did not expect calendar table", sqlText)
	}
}

func TestPreferBuilderSQLRebuildsLatestMonthHalfComparison(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select 'first half' as period_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics"

	sqlText, rebuilt := preferBuilderSQLForIntent("Сравни первую и вторую половину последнего доступного месяца по выручке, завершённым поездкам, средней стоимости и доле отмен.", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected latest month half comparison to be rebuilt")
	}
	for _, needle := range []string{
		"первая половина последнего доступного месяца",
		"вторая половина последнего доступного месяца",
		"revenue_value",
		"completed_orders_value",
		"avg_price_value",
		"cancellation_rate_value",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("sqlText = %q, want %q", sqlText, needle)
		}
	}
}

func TestPreferBuilderSQLRebuildsNamedWeekComparison(t *testing.T) {
	intent := shared.Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.91,
	}
	modelSQL := "select 'bad' as period_value, sum(gross_revenue_local) as metric_value from analytics.v_ride_metrics"

	sqlText, rebuilt := preferBuilderSQLForIntent("Сравни выручку за последнюю неделю марта 2025 и первую неделю апреля 2025", intent, modelSQL)
	if !rebuilt {
		t.Fatal("expected named week comparison to be rebuilt")
	}
	for _, needle := range []string{
		"date '2025-03-25'",
		"date '2025-03-31'",
		"date '2025-04-01'",
		"date '2025-04-07'",
		"Последняя неделя марта 2025",
		"Первая неделя апреля 2025",
	} {
		if !strings.Contains(sqlText, needle) {
			t.Fatalf("sqlText = %q, want %q", sqlText, needle)
		}
	}
}

func TestUserSpecifiedPeriodDoesNotTreatGroupByMonthAsDateFilter(t *testing.T) {
	if userSpecifiedPeriod("Покажи выручку по месяцам") {
		t.Fatal("did not expect grouping by months to count as an explicit period")
	}
	if !userSpecifiedPeriod("Покажи выручку за месяц") {
		t.Fatal("expected 'за месяц' to count as an explicit period")
	}
}
