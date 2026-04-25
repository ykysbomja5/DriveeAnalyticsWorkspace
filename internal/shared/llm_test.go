package shared

import "testing"

func TestValidateGeneratedSQLAllowsApprovedAnalyticsSources(t *testing.T) {
	testCases := []string{
		"select city_id as group_value, count(*) as metric_value from analytics.incity group by 1 limit 10",
		"select city as group_value, round(sum(gross_revenue_local)::numeric, 2) as metric_value from analytics.v_ride_metrics group by 1",
		"select date_trunc('day', vm.stat_date)::date as period_value, round(sum(vm.avg_distance_meters * vm.completed_orders) / nullif(sum(vm.completed_orders), 0), 2) as metric_value from analytics.v_ride_metrics vm group by 1 order by 1",
		"select city as group_value, round(avg(final_price_local)::numeric, 2) as metric_value from analytics.v_incity_orders_latest where completed_orders = 1 group by city limit 100",
		"select dm.stat_date as period_value, sum(dm.active_drivers)::integer as metric_value from analytics.v_driver_daily_metrics dm group by dm.stat_date order by period_value limit 100",
		"select pm.city as group_value, round(avg(pm.acceptance_rate)::numeric, 4) as metric_value from analytics.v_passenger_daily_metrics pm group by pm.city limit 100",
		"select city_id as group_value, count(distinct driver_id)::integer as metric_value from analytics.driver_detail group by city_id limit 100",
		"select city_id as group_value, count(distinct user_id)::integer as metric_value from analytics.pass_detail group by city_id limit 100",
		"select concat('driver_', substr(encode(digest(d.driver_id, 'sha256'), 'hex'), 1, 12)) as group_value, sum(d.rides_count)::integer as metric_value from analytics.driver_detail d where d.city_id = '60' group by concat('driver_', substr(encode(digest(d.driver_id, 'sha256'), 'hex'), 1, 12)) limit 100",
		"select concat('client_', substr(encode(digest(p.user_id, 'sha256'), 'hex'), 1, 12)) as group_value, sum(p.orders_count)::integer as metric_value from analytics.pass_detail p group by concat('client_', substr(encode(digest(p.user_id, 'sha256'), 'hex'), 1, 12)) limit 100",
	}

	for _, sqlText := range testCases {
		if err := ValidateGeneratedSQL(sqlText); err != nil {
			t.Fatalf("ValidateGeneratedSQL(%q) error = %v", sqlText, err)
		}
	}
}

func TestValidateGeneratedSQLRejectsUnknownAnalyticsSource(t *testing.T) {
	testCases := []string{
		"select city as group_value from analytics.some_other_table",
		"select city as group_value from public.users",
	}

	for _, sqlText := range testCases {
		if err := ValidateGeneratedSQL(sqlText); err == nil {
			t.Fatalf("ValidateGeneratedSQL(%q) unexpectedly succeeded", sqlText)
		}
	}
}

func TestValidateGeneratedSQLRejectsSensitiveIdentifierProjection(t *testing.T) {
	testCases := []string{
		"select driver_id as group_value from analytics.driver_detail",
		"select user_id as group_value from analytics.pass_detail",
		"select d.driver_id, count(*) as metric_value from analytics.driver_detail d group by d.driver_id",
	}

	for _, sqlText := range testCases {
		if err := ValidateGeneratedSQL(sqlText); err == nil {
			t.Fatalf("ValidateGeneratedSQL(%q) unexpectedly succeeded", sqlText)
		}
	}
}

func TestValidateGeneratedSQLRejectsWildcardSelect(t *testing.T) {
	testCases := []string{
		`select * from analytics.v_ride_metrics`,
		`select vm.* from analytics.v_ride_metrics vm`,
		`select vm.city, * from analytics.v_ride_metrics vm`,
	}

	for _, sqlText := range testCases {
		if err := ValidateGeneratedSQL(sqlText); err == nil {
			t.Fatalf("expected wildcard select to be blocked for %q", sqlText)
		}
	}
}

func TestValidateGeneratedSQLRejectsColumnOutsideAllowlist(t *testing.T) {
	sqlText := `select vm.secret_metric from analytics.v_ride_metrics vm`
	if err := ValidateGeneratedSQL(sqlText); err == nil {
		t.Fatal("expected unknown column to be blocked")
	}
}

func TestValidateGeneratedSQLRejectsOverlyComplexQuery(t *testing.T) {
	sqlText := `
with a as (select city, total_orders from analytics.v_ride_metrics),
     b as (select city, completed_orders from analytics.v_ride_metrics),
     c as (select city, cancelled_orders from analytics.v_ride_metrics),
     d as (select city, gross_revenue_local from analytics.v_ride_metrics),
     e as (select city, avg_price_local from analytics.v_ride_metrics)
select a.city, a.total_orders
from a
join b on b.city = a.city
join c on c.city = a.city
join d on d.city = a.city
join e on e.city = a.city
`
	if err := ValidateGeneratedSQL(sqlText); err == nil {
		t.Fatal("expected complex query to be blocked")
	}
}

func TestValidateGeneratedSQLAllowsPeriodComparisonWithoutUnion(t *testing.T) {
	sqlText := `with periods as (
  values
    (1, 'первые 7 дней марта 2025', date '2025-03-01', (date '2025-03-01' + interval '6 days')::date),
    (2, 'последние 7 дней марта 2025', ((date '2025-03-01' + interval '1 month' - interval '1 day')::date - interval '6 days')::date, (date '2025-03-01' + interval '1 month' - interval '1 day')::date)
)
select p.column2 as period_value,
       coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value
from periods p
left join analytics.v_ride_metrics vm on vm.stat_date between p.column3 and p.column4
group by p.column1, p.column2
order by p.column1
limit 100`
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("expected period comparison query to pass guardrails, got %v", err)
	}
}

func TestValidateGeneratedSQLAllowsCTEColumnList(t *testing.T) {
	sqlText := `with periods(period_sort, period_value, from_date, to_date) as (
  values
    (1, 'первые 7 дней марта 2025', date '2025-03-01', date '2025-03-07'),
    (2, 'последние 7 дней марта 2025', date '2025-03-25', date '2025-03-31')
)
select p.period_value as period_value,
       coalesce(round(sum(vm.gross_revenue_local)::numeric, 2), 0) as metric_value
from periods p
left join analytics.v_ride_metrics vm on vm.stat_date between p.from_date and p.to_date
group by p.period_sort, p.period_value
order by p.period_sort
limit 100`
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("expected CTE with column list to pass guardrails, got %v", err)
	}
}

func TestValidateGeneratedSQLAllowsPriceThresholdShareWithDataAnchoredMonth(t *testing.T) {
	sqlText := `with bounds as (
  select max(stat_date) as max_date
  from analytics.v_ride_metrics
)
select
  round(100.0 * sum(case when o.final_price_local > 500 then 1 else 0 end)::numeric / nullif(count(*), 0), 2) as metric_value
from analytics.v_incity_orders_latest o
join bounds b on true
where o.stat_date between (b.max_date - interval '1 month' + interval '1 day')::date and b.max_date
limit 1`
	if err := ValidateGeneratedSQL(sqlText); err != nil {
		t.Fatalf("expected price threshold percentage query to pass guardrails, got %v", err)
	}
}
