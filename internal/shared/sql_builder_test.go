package shared

import (
	"strings"
	"testing"
)

func TestBuildSQLFromIntentUsesCalendarSeriesForDailyRanges(t *testing.T) {
	intent := Intent{
		Metric:  "revenue",
		GroupBy: "day",
		Period: TimeRange{
			From:  "2026-03-27",
			To:    "2026-04-23",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Покажи динамику выручки за последние 28 дней", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"with anchor as ( select max(stat_date)::date as max_date from analytics.v_ride_metrics )",
		"select generate_series( (select max_date - interval '27 day' from anchor), (select max_date from anchor), interval '1 day' )::date as period_value",
		"left join analytics.v_ride_metrics vm on vm.stat_date = series.period_value",
		"series.period_value::text as period_value",
		"coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value",
		"group by series.period_value",
		"order by series.period_value asc",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestInclusiveDayCount(t *testing.T) {
	count, ok := inclusiveDayCount("2026-04-21", "2026-04-23")
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if count != 3 {
		t.Fatalf("count=%d, want 3", count)
	}
}

func TestBuildSQLFromIntentUsesQualifiedWeekGrouping(t *testing.T) {
	intent := Intent{
		Metric:  "revenue",
		GroupBy: "week",
		Period: TimeRange{
			From:  "2025-04-01",
			To:    "2025-04-30",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Сравни выручку по неделям в апреле 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"from analytics.v_ride_metrics vm",
		"date_trunc('week', vm.stat_date)::date as period_value",
		"round(sum(vm.gross_revenue_local)::numeric, 2) as metric_value",
		"where vm.stat_date between date '2025-04-01' and date '2025-04-30'",
		"group by date_trunc('week', vm.stat_date)::date",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentUsesWholeDatasetWhenPeriodIsMissing(t *testing.T) {
	intent := Intent{Metric: "revenue", GroupBy: "city"}

	sqlText, err := BuildSQLFromIntent("Покажи выручку по городам", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}
	if strings.Contains(sqlText, "stat_date between") {
		t.Fatalf("unexpected date filter in sql: %s", sqlText)
	}
}

func TestBuildSQLFromIntentSkipsInvalidRelativeDateBounds(t *testing.T) {
	intent := Intent{
		Metric:  "avg_duration_minutes",
		GroupBy: "day",
		Period: TimeRange{
			From:  "now() - '12 days'::interval",
			To:    "now()",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Покажи среднюю длительность завершенного заказа по дням за последние 12 дней", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}
	if strings.Contains(sqlText, "now() -") || strings.Contains(sqlText, "date 'now()") {
		t.Fatalf("invalid relative date leaked into sql: %s", sqlText)
	}
	if strings.Contains(sqlText, "stat_date between") {
		t.Fatalf("unexpected invalid date filter in sql: %s", sqlText)
	}
}

func TestBuildSQLFromIntentBuildsTwoPeriodComparison(t *testing.T) {
	intent := Intent{Metric: "avg_price"}

	sqlText, err := BuildSQLFromIntent("Сравни среднюю стоимость завершенных заказов за первые 10 дней апреля 2025 и последние 10 дней апреля 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"from analytics.v_ride_metrics vm",
		"when vm.stat_date between date '2025-04-01' and date '2025-04-10' then 'Первые 10 дней апреля 2025'",
		"when vm.stat_date between date '2025-04-21' and date '2025-04-30' then 'Последние 10 дней апреля 2025'",
		"round(sum(vm.gross_revenue_local) / nullif(sum(vm.completed_orders), 0), 2) as metric_value",
		"order by min(vm.stat_date)",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsDifferentMonthEdgeComparison(t *testing.T) {
	intent := Intent{Metric: "avg_price"}

	sqlText, err := BuildSQLFromIntent("Сравни среднюю стоимость завершенных заказов за первые 17 дней августа 2025 и последние 10 дней апреля 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"Первые 17 дней августа 2025",
		"Последние 10 дней апреля 2025",
		"stat_date between date '2025-08-01' and date '2025-08-17'",
		"stat_date between date '2025-04-21' and date '2025-04-30'",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsMultiMonthComparison(t *testing.T) {
	intent := Intent{Metric: "cancellations"}

	sqlText, err := BuildSQLFromIntent("Сравни количество отмененных заказов за Апрель, Май и Июнь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"from analytics.v_ride_metrics vm",
		"when vm.stat_date between date '2025-04-01' and date '2025-04-30' then 'Апреля 2025'",
		"when vm.stat_date between date '2025-05-01' and date '2025-05-31' then 'Мая 2025'",
		"when vm.stat_date between date '2025-06-01' and date '2025-06-30' then 'Июня 2025'",
		"sum(vm.cancelled_orders) as metric_value",
		"order by min(vm.stat_date)",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsMonthlyWindowComparison(t *testing.T) {
	intent := Intent{Metric: "avg_price", GroupBy: "month"}

	sqlText, err := BuildSQLFromIntent("Сравни среднюю стоимость завершенных заказов за первые 10 дней каждого месяца с января по июнь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"with periods(period_value, period_order, from_date, to_date) as",
		"('Первые 10 дней января 2025', 1, date '2025-01-01', date '2025-01-10')",
		"('Первые 10 дней июня 2025', 6, date '2025-06-01', date '2025-06-10')",
		"left join analytics.v_ride_metrics vm on vm.stat_date between p.from_date and p.to_date",
		"coalesce(round(sum(vm.gross_revenue_local) / nullif(sum(vm.completed_orders), 0), 2), 0) as metric_value",
		"group by p.period_order, p.period_value",
		"order by p.period_order",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}

	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsNamedWeekComparison(t *testing.T) {
	intent := Intent{Metric: "revenue", GroupBy: "week"}

	sqlText, err := BuildSQLFromIntent("Сравни выручку за последнюю неделю марта 2025 и первую неделю апреля 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"when vm.stat_date between date '2025-03-25' and date '2025-03-31' then 'Последняя неделя марта 2025'",
		"when vm.stat_date between date '2025-04-01' and date '2025-04-07' then 'Первая неделя апреля 2025'",
		"round(sum(vm.gross_revenue_local)::numeric, 2) as metric_value",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentUsesFirstDaysOfMonthRange(t *testing.T) {
	intent := Intent{
		Metric:  "revenue",
		GroupBy: "day",
		Period: TimeRange{
			Label: "первые 6 дней августа 2025",
			From:  "2025-08-01",
			To:    "2025-08-06",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Покажи выручку по дням за первые 6 дней Августа 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}
	compact := compactSQL(sqlText)
	for _, needle := range []string{"date '2025-08-01'", "date '2025-08-06'", "interval '1 day'"} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsCancellationRateTimeSeries(t *testing.T) {
	intent := Intent{
		Metric:  "cancellation_rate",
		GroupBy: "day",
		Period: TimeRange{
			From:  "2025-04-10",
			To:    "2025-04-23",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Покажи соотношение отмен к завершенным заказам по дням за последние 2 недели", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"left join analytics.v_ride_metrics vm on vm.stat_date = series.period_value",
		"coalesce(round(sum(vm.cancelled_orders)::numeric / nullif(sum(vm.completed_orders), 0), 4), 0) as metric_value",
		"group by series.period_value",
		"order by series.period_value asc",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsMetricMovementFilter(t *testing.T) {
	intent := Intent{
		Metric:  "completed_orders",
		GroupBy: "day",
		Pattern: "metric_movement_filter",
		Period: TimeRange{
			From:  "2025-10-01",
			To:    "2025-10-31",
			Grain: "day",
		},
		MovementConditions: []MetricMovementCondition{
			{Metric: "completed_orders", Direction: "down"},
			{Metric: "cancellations", Direction: "up"},
		},
	}

	sqlText, err := BuildSQLFromIntent("Найди дни, где завершенные заказы просели, а отмены выросли за Октябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"date '2025-10-01' - interval '1 day'",
		"date '2025-10-31'",
		"coalesce(sum(vm.completed_orders), 0) as completed_orders_value",
		"coalesce(sum(vm.cancelled_orders), 0) as cancellations_value",
		"completed_orders_value - lag(completed_orders_value) over (order by stat_date) as completed_orders_delta",
		"cancellations_value - lag(cancellations_value) over (order by stat_date) as cancellations_delta",
		"movement.completed_orders_delta < 0",
		"movement.cancellations_delta > 0",
		"order by movement.stat_date asc",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}

	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentOrdersLimitedTimeGroupingByMetric(t *testing.T) {
	intent := Intent{
		Metric:  "total_orders",
		GroupBy: "day",
		Limit:   1,
		Period: TimeRange{
			Label: "весь доступный период",
		},
	}

	sqlText, err := BuildSQLFromIntent("Найди день с самым большим количеством заказов", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"date_trunc('day', vm.stat_date)::date as period_value",
		"sum(vm.total_orders) as metric_value",
		"group by date_trunc('day', vm.stat_date)::date",
		"order by metric_value desc, date_trunc('day', vm.stat_date)::date asc",
		"limit 1",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func TestBuildSQLFromIntentBuildsOrderPriceThresholdShare(t *testing.T) {
	intent := Intent{
		Metric:  "order_price_threshold_rate",
		Pattern: "order_price_threshold_share",
		Period: TimeRange{
			From:  "2025-10-01",
			To:    "2025-10-31",
			Grain: "day",
		},
	}

	sqlText, err := BuildSQLFromIntent("Какой процент заказов со средней стоимостью выше 300 рублей за последний месяц", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	compact := compactSQL(sqlText)
	for _, needle := range []string{
		"from analytics.incity io",
		"count(distinct case when coalesce(io.price_order_local, io.price_tender_local, io.price_start_local, 0) > 300 then io.order_id end)::numeric",
		"nullif(count(distinct io.order_id), 0)",
		"as metric_value",
		"io.order_timestamp::date between date '2025-10-01' and date '2025-10-31'",
	} {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}

	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsCheapestDailyTrips(t *testing.T) {
	intent := Intent{
		Metric:     "avg_price",
		Limit:      6,
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Покажи 6 самых дешевых отдельных поездок за 30 дней марта 2026", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"row_number() over ( partition by o.stat_date order by o.final_price_local asc ) as price_rank",
		"from analytics.v_incity_orders_latest o",
		"o.stat_date between date '2026-03-01' and date '2026-03-30'",
		"o.final_price_local is not null",
		"o.completed_orders = 1",
		"where price_rank = 1",
		"period_value::text as period_value",
		"metric_value",
		"group_value",
		"order by metric_value asc, period_value asc",
		"limit 6",
	)
	mustNotContain(t, sqlText, "date '2026-03-31'")
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsMetricThresholdDaysForCalendarMonth(t *testing.T) {
	intent := Intent{
		Metric:     "avg_price",
		GroupBy:    "day",
		Confidence: 0.92,
	}

	sqlText, err := BuildSQLFromIntent("Покажи дни, где средняя цена заказа больше 300 рублей за ноябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"o.stat_date as period_value",
		"round(avg(o.final_price_local)::numeric, 2) as metric_value",
		"from analytics.v_incity_orders_latest o",
		"where o.stat_date between date '2025-11-01' and date '2025-11-30' and o.final_price_local is not null",
		"group by o.stat_date",
		"having round(avg(o.final_price_local)::numeric, 2) > 300",
		"limit 100",
	)
	mustNotContain(t, sqlText, "max(stat_date)")
	mustNotContain(t, sqlText, "city is not null")
}

func TestBuildSQLFromIntentBuildsPriceDistributionHistogram(t *testing.T) {
	intent := Intent{
		Metric:     "avg_price",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Покажи гистограмму стоимости заказов за ноябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"from analytics.v_incity_orders_latest o",
		"width_bucket(o.final_price_local::numeric",
		"o.stat_date between date '2025-11-01' and date '2025-11-30'",
		"as bucket_value",
		"count(*)::integer as metric_value",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsRollingPreviousPeriodComparison(t *testing.T) {
	intent := Intent{
		Metric:     "avg_price",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Покажи гистограммой сравнение средней стоимости поездки за последние 14 дней в данных и предыдущие 14 дней до них.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"select max(stat_date)::date as max_date",
		"'предыдущие 14 дней'",
		"interval '27 days'",
		"interval '14 days'",
		"'последние 14 дней'",
		"interval '13 days'",
		"p.column2 as period_value",
		"coalesce(round(sum(vm.gross_revenue_local) / nullif(sum(vm.completed_orders), 0), 2), 0) as metric_value",
	)
	mustNotContain(t, sqlText, "width_bucket")
	mustNotContain(t, sqlText, "bucket_value")
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsRollingCityMovementComparison(t *testing.T) {
	intent := Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Покажи города, где за последние 30 дней в данных выручка выросла относительно предыдущих 30 дней, но доля отмен тоже выросла.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"select max(stat_date)::date as max_date from analytics.v_ride_metrics",
		"vm.city as group_value",
		"interval '29 days'",
		"interval '59 days'",
		"interval '30 days'",
		"current_revenue as metric_value",
		"previous_revenue",
		"revenue_delta",
		"current_cancellation_rate",
		"previous_cancellation_rate",
		"cancellation_rate_delta",
		"where revenue_delta > 0 and cancellation_rate_delta > 0",
		"order by revenue_delta desc, group_value asc",
		"limit 100",
	)
	mustNotContain(t, sqlText, "p.column2 as period_value")
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsRollingMultiMetricComparison(t *testing.T) {
	intent := Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Сравни последние 30 дней в данных с предыдущими 30 днями по выручке, завершённым поездкам, отменам, средней стоимости и доле отмен.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"select max(stat_date)::date as max_date from analytics.v_ride_metrics",
		"'предыдущие 30 дней'",
		"'последние 30 дней'",
		"interval '59 days'",
		"interval '30 days'",
		"interval '29 days'",
		"p.period_value",
		"as metric_value",
		"as revenue_value",
		"as completed_orders_value",
		"as cancellations_value",
		"as avg_price_value",
		"sum(vm.cancelled_orders)::numeric / nullif(sum(vm.total_orders), 0)",
		"as cancellation_rate_value",
		"limit 100",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsCityMultiMetricComparison(t *testing.T) {
	intent := Intent{
		Metric:     "revenue",
		GroupBy:    "city",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Сравни города по выручке, завершённым поездкам, отменам, средней стоимости и доле отмен.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"vm.city as group_value",
		"as metric_value",
		"as revenue_value",
		"as completed_orders_value",
		"as cancellations_value",
		"as avg_price_value",
		"as cancellation_rate_value",
		"from analytics.v_ride_metrics vm",
		"where vm.city is not null",
		"group by vm.city",
		"order by revenue_value desc, group_value asc",
		"limit 100",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsWeekdayBestWorst(t *testing.T) {
	intent := Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Определи лучшие и худшие дни недели по выручке, количеству завершённых поездок и средней стоимости за последние 90 дней в данных.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"from analytics.v_ride_metrics",
		"date_part('isodow', vm.stat_date)::integer as weekday_number",
		"weekday_value as group_value",
		"revenue_value as metric_value",
		"completed_orders_value",
		"avg_price_value",
		"revenue_best_rank",
		"revenue_worst_rank",
		"completed_orders_best_rank",
		"completed_orders_worst_rank",
		"avg_price_best_rank",
		"avg_price_worst_rank",
		"interval '89 days'",
		"limit 100",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsLatestMonthHalfComparison(t *testing.T) {
	intent := Intent{
		Metric:     "revenue",
		Pattern:    "comparison",
		Confidence: 0.9,
	}

	sqlText, err := BuildSQLFromIntent("Сравни первую и вторую половину последнего доступного месяца по выручке, завершённым поездкам, средней стоимости и доле отмен.", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"date_trunc('month', max(stat_date))::date as month_start",
		"'первая половина последнего доступного месяца'",
		"'вторая половина последнего доступного месяца'",
		"least(((select month_start from bounds) + interval '14 days')::date, (select max_date from bounds))",
		"((select month_start from bounds) + interval '15 days')::date",
		"as revenue_value",
		"as completed_orders_value",
		"as avg_price_value",
		"as cancellation_rate_value",
		"where p.from_date <= p.to_date",
		"limit 100",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestLooksLikeDistributionRequestDoesNotTreatHistogramComparisonAsDistribution(t *testing.T) {
	if LooksLikeDistributionRequest("Покажи гистограммой сравнение средней стоимости поездки за последние 14 дней и предыдущие 14 дней") {
		t.Fatal("expected histogram comparison to stay a comparison, not a distribution")
	}
	if !LooksLikeDistributionRequest("Покажи гистограмму распределения стоимости заказов") {
		t.Fatal("expected explicit distribution histogram")
	}
}

func TestBuildSQLFromIntentBuildsDistanceDistributionHistogram(t *testing.T) {
	sqlText, err := BuildSQLFromIntent("Покажи распределение поездок по дистанции", Intent{Metric: "avg_distance_meters"})
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"width_bucket(o.distance_in_meters::numeric",
		"o.distance_in_meters is not null",
		"as bucket_value",
		"count(*)::integer as metric_value",
	)
}

func TestBuildSQLFromIntentBuildsCompletedOrderAveragePriceThresholdWhenExplicit(t *testing.T) {
	intent := Intent{
		Metric:     "avg_price",
		GroupBy:    "day",
		Confidence: 0.92,
	}

	sqlText, err := BuildSQLFromIntent("Покажи дни, где средняя цена завершенных заказов больше 300 рублей за ноябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"from analytics.v_incity_orders_latest o",
		"o.completed_orders = 1",
		"having round(avg(o.final_price_local)::numeric, 2) > 300",
	)
}

func TestBuildSQLFromIntentBuildsDistinctDriverMetricsFromDetail(t *testing.T) {
	intent := Intent{
		Metric:  "active_drivers",
		GroupBy: "city",
		Period:  TimeRange{From: "2025-10-01", To: "2025-10-31", Grain: "day"},
	}

	sqlText, err := BuildSQLFromIntent("Сколько активных водителей по городам за октябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"from analytics.driver_detail vm",
		"vm.city_id as group_value",
		"count(distinct vm.driver_id) as metric_value",
		"vm.tender_date_part between date '2025-10-01' and date '2025-10-31'",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentInfersDriverBreakdownFromText(t *testing.T) {
	intent := Intent{
		Metric:  "completed_rides",
		Filters: []Filter{{Field: "city_id", Operator: "=", Value: "60"}},
	}

	sqlText, err := BuildSQLFromIntent("Покажи статистику завершенных поездок с разбивкой по водителям в городе 60", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"from analytics.driver_detail vm",
		"concat('driver_', substr(encode(digest(vm.driver_id, 'sha256'), 'hex'), 1, 12)) as group_value",
		"sum(vm.rides_count) as metric_value",
		"vm.city_id = '60'",
		"group by concat('driver_', substr(encode(digest(vm.driver_id, 'sha256'), 'hex'), 1, 12))",
	)
	if strings.Contains(sqlText, "vm.driver_id as") {
		t.Fatalf("sql exposes raw driver_id: %s", sqlText)
	}
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestBuildSQLFromIntentBuildsClientMetricsFromPassengerDetail(t *testing.T) {
	intent := Intent{
		Metric:  "avg_online_time_seconds",
		GroupBy: "day",
		Period:  TimeRange{Label: "октябрь 2025", From: "2025-10-01", To: "2025-10-03", Grain: "day"},
	}

	sqlText, err := BuildSQLFromIntent("Покажи среднее онлайн-время клиентов по дням за октябрь 2025", intent)
	if err != nil {
		t.Fatalf("BuildSQLFromIntent() error = %v", err)
	}

	mustContainAll(t, sqlText,
		"left join analytics.pass_detail vm on vm.order_date_part = series.period_value",
		"round(sum(vm.online_time_sum_seconds)::numeric / nullif(count(distinct vm.user_id), 0), 2)",
		"series.period_value::text as period_value",
	)
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("generated SQL did not pass validation: %v\n%s", err, sqlText)
	}
}

func TestDetectOrderPriceThresholdShare(t *testing.T) {
	threshold, ok := DetectOrderPriceThresholdShare("Какой процент заказов со средней стоимостью выше 300 рублей за последний месяц")
	if !ok {
		t.Fatalf("expected threshold share detection")
	}
	if threshold.Operator != ">" || threshold.Threshold != 300 {
		t.Fatalf("threshold=%+v, want > 300", threshold)
	}
}

func TestDetectMetricMovementConditions(t *testing.T) {
	conditions, ok := DetectMetricMovementConditions("Найди дни, где завершенные заказы просели, а отмены выросли за Октябрь 2025")
	if !ok {
		t.Fatalf("expected movement conditions")
	}
	if len(conditions) != 2 {
		t.Fatalf("conditions len=%d, want 2: %+v", len(conditions), conditions)
	}
	if conditions[0] != (MetricMovementCondition{Metric: "completed_orders", Direction: "down"}) {
		t.Fatalf("first condition=%+v", conditions[0])
	}
	if conditions[1] != (MetricMovementCondition{Metric: "cancellations", Direction: "up"}) {
		t.Fatalf("second condition=%+v", conditions[1])
	}
}

func mustContainAll(t *testing.T, sqlText string, needles ...string) {
	t.Helper()
	compact := compactSQL(sqlText)
	for _, needle := range needles {
		if !strings.Contains(compact, needle) {
			t.Fatalf("sql does not contain %q: %s", needle, compact)
		}
	}
}

func mustNotContain(t *testing.T, sqlText, needle string) {
	t.Helper()
	if strings.Contains(compactSQL(sqlText), needle) {
		t.Fatalf("sql unexpectedly contains %q: %s", needle, compactSQL(sqlText))
	}
}
