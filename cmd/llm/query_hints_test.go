package main

import (
	"strings"
	"testing"
)

func TestInferQueryHintsTreatsGraphAsDailyTimeSeries(t *testing.T) {
	hints := inferQueryHints("Построй график выручки за месяц")

	if hints.Metric != "revenue" {
		t.Fatalf("Metric = %q, want revenue", hints.Metric)
	}
	if !hasGroup(hints.GroupBys, "day") {
		t.Fatalf("GroupBys = %v, want day", hints.GroupBys)
	}
	if hints.ExpectedColumns != "period_value, metric_value" {
		t.Fatalf("ExpectedColumns = %q, want period_value, metric_value", hints.ExpectedColumns)
	}
	if hints.Visualization != "area-line" {
		t.Fatalf("Visualization = %q, want area-line", hints.Visualization)
	}
	if !hints.PeriodSpecified {
		t.Fatal("expected period to be detected for 'за месяц'")
	}
}

func TestInferQueryHintsMarksMissingPeriodAsWholeDataset(t *testing.T) {
	message := buildQwenUserMessage("Покажи выручку по городам")

	if !strings.Contains(message, "Период в запросе явно не указан") {
		t.Fatalf("message = %q, want missing period hint", message)
	}
	if !strings.Contains(message, "весь доступный период") {
		t.Fatalf("message = %q, want whole dataset period hint", message)
	}
}

func TestInferQueryHintsDetectsDistributionVisualization(t *testing.T) {
	hints := inferQueryHints("Покажи гистограмму стоимости заказов за ноябрь 2025")

	if hints.Pattern != "distribution" {
		t.Fatalf("Pattern = %q, want distribution", hints.Pattern)
	}
	if hints.Visualization != "histogram" {
		t.Fatalf("Visualization = %q, want histogram", hints.Visualization)
	}
	if hints.ExpectedColumns != "bucket_value, metric_value" {
		t.Fatalf("ExpectedColumns = %q, want bucket_value, metric_value", hints.ExpectedColumns)
	}
	if len(hints.GroupBys) != 0 {
		t.Fatalf("GroupBys = %v, want no time/category grouping for histogram", hints.GroupBys)
	}
}

func TestInferQueryHintsTreatsHistogramComparisonAsBarComparison(t *testing.T) {
	hints := inferQueryHints("Покажи гистограммой сравнение средней стоимости поездки за последние 14 дней в данных и предыдущие 14 дней до них.")

	if hints.Pattern != "comparison" {
		t.Fatalf("Pattern = %q, want comparison", hints.Pattern)
	}
	if hints.Visualization != "bar" {
		t.Fatalf("Visualization = %q, want bar", hints.Visualization)
	}
	if hints.ExpectedColumns != "period_value, metric_value" {
		t.Fatalf("ExpectedColumns = %q, want period_value, metric_value", hints.ExpectedColumns)
	}
	if len(hints.GroupBys) != 0 {
		t.Fatalf("GroupBys = %v, want no time/category grouping for comparison", hints.GroupBys)
	}
}

func TestInferQueryHintsDetectsMultiMetricComparison(t *testing.T) {
	hints := inferQueryHints("Сравни последние 30 дней в данных с предыдущими 30 днями по выручке, завершённым поездкам, отменам, средней стоимости и доле отмен.")

	if hints.Visualization != "table" {
		t.Fatalf("Visualization = %q, want table", hints.Visualization)
	}
	for _, needle := range []string{"revenue_value", "completed_orders_value", "cancellations_value", "avg_price_value", "cancellation_rate_value"} {
		if !strings.Contains(hints.ExpectedColumns, needle) {
			t.Fatalf("ExpectedColumns = %q, want %q", hints.ExpectedColumns, needle)
		}
	}
}

func TestInferQueryHintsDetectsTopCityCancellations(t *testing.T) {
	hints := inferQueryHints("Топ-3 города по количеству отменённых заказов на этой неделе")

	if hints.Metric != "cancellations" {
		t.Fatalf("Metric = %q, want cancellations", hints.Metric)
	}
	if !hasGroup(hints.GroupBys, "city") {
		t.Fatalf("GroupBys = %v, want city", hints.GroupBys)
	}
	if hints.Limit != 3 {
		t.Fatalf("Limit = %d, want 3", hints.Limit)
	}
	if hints.ExpectedColumns != "group_value, metric_value" {
		t.Fatalf("ExpectedColumns = %q, want group_value, metric_value", hints.ExpectedColumns)
	}
}

func TestInferQueryHintsDetectsPriceThresholdRate(t *testing.T) {
	hints := inferQueryHints("Какой процент заказов стоимостью выше 500 рублей за месяц")

	if hints.Metric != "order_price_threshold_rate" {
		t.Fatalf("Metric = %q, want order_price_threshold_rate", hints.Metric)
	}
	if hints.PriceOperator != ">" || hints.PriceThreshold != "500" {
		t.Fatalf("Price filter = %q %q, want > 500", hints.PriceOperator, hints.PriceThreshold)
	}
	if hints.ExpectedColumns != "metric_value" {
		t.Fatalf("ExpectedColumns = %q, want metric_value", hints.ExpectedColumns)
	}
}

func TestInferQueryHintsPrefersThresholdNumberNearOperator(t *testing.T) {
	hints := inferQueryHints("За последние 7 дней какой процент заказов выше 500 рублей")

	if hints.PriceOperator != ">" || hints.PriceThreshold != "500" {
		t.Fatalf("Price filter = %q %q, want > 500", hints.PriceOperator, hints.PriceThreshold)
	}
}
