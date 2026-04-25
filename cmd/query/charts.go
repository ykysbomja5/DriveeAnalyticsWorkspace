package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
)

// chooseChart выводит аналитическое поведение из текста пользователя или результата.
func chooseChart(intent shared.Intent, result shared.QueryResult) shared.ChartSpec {
	if len(result.Columns) < 2 {
		return shared.ChartSpec{
			Type:     "metric",
			Headline: intent.MetricLabel(shared.DefaultSemanticLayer()),
			Subtitle: intent.Period.Label,
		}
	}

	highlights := chartHighlights(result)
	headline := intent.MetricLabel(shared.DefaultSemanticLayer())
	if label := intent.GroupByLabel(shared.DefaultSemanticLayer()); label != "" {
		headline = fmt.Sprintf("%s по %s", headline, strings.ToLower(label))
	}

	xKey := result.Columns[0]
	yKey := result.Columns[1]
	if len(result.Columns) >= 3 {
		xKey = result.Columns[0]
		yKey = result.Columns[2]
	}

	spec := shared.ChartSpec{
		XKey:       xKey,
		YKey:       yKey,
		Headline:   headline,
		Subtitle:   intent.Period.Label,
		Highlights: highlights,
	}

	if shared.IsMultiMetricResultColumns(result.Columns) {
		spec.Type = "table"
		spec.SecondaryType = ""
		spec.Headline = "Сравнение нескольких метрик"
		return spec
	}

	if isDistributionResult(result) || strings.Contains(strings.ToLower(strings.TrimSpace(intent.Pattern)), "distribution") {
		spec.Type = "histogram"
		spec.SecondaryType = "share-donut"
		spec.Headline = distributionHeadline(intent, result)
		return spec
	}

	if isTimeSeriesResult(result) {
		spec.Type = "area-line"
		spec.SecondaryType = "share-donut"
		return spec
	}

	if strings.Contains(strings.ToLower(strings.TrimSpace(intent.Pattern)), "comparison") {
		spec.Type = "bar"
		spec.SecondaryType = "share-donut"
		return spec
	}

	// Для временной группировки (день/неделя/месяц) используем area-line chart.
	switch intent.GroupBy {
	case "day", "week", "month":
		spec.Type = "area-line"
		spec.SecondaryType = "share-donut"
		return spec
	default:
		// Для категорий или малых наборов (2-10 строк) используем bar и donut.
		if len(result.Rows) >= 2 && len(result.Rows) <= 10 {
			spec.Type = "bar"
			spec.SecondaryType = "share-donut"
			return spec
		}
		// Для больших наборов используем lollipop и donut.
		if len(result.Rows) >= 11 {
			spec.Type = "lollipop"
			spec.SecondaryType = "share-donut"
			return spec
		}
		// Запасная ветка для пограничных случаев.
		spec.Type = "bar"
		spec.SecondaryType = "share-donut"
		return spec
	}
}

func distributionHeadline(intent shared.Intent, result shared.QueryResult) string {
	metric := strings.ToLower(strings.TrimSpace(intent.Metric))
	switch metric {
	case "avg_distance_meters":
		return "Распределение заказов по дистанции"
	case "avg_duration_minutes":
		return "Распределение заказов по длительности"
	case "avg_price", "order_price_threshold_rate":
		return "Распределение заказов по стоимости"
	default:
		if len(result.Columns) > 0 && strings.Contains(strings.ToLower(result.Columns[0]), "bucket") {
			return "Распределение заказов"
		}
		return intent.MetricLabel(shared.DefaultSemanticLayer())
	}
}

func isDistributionResult(result shared.QueryResult) bool {
	if len(result.Columns) < 2 {
		return false
	}
	xKey := strings.ToLower(strings.TrimSpace(result.Columns[0]))
	return xKey == "bucket_value" || xKey == "bucket_label" || xKey == "bucket_number"
}

// isTimeSeriesResult централизует проверку для последующего ветвления логики.
func isTimeSeriesResult(result shared.QueryResult) bool {
	if len(result.Columns) < 2 || len(result.Rows) == 0 {
		return false
	}

	xKey := strings.ToLower(strings.TrimSpace(result.Columns[0]))
	if xKey == "period_value" || xKey == "stat_date" || xKey == "period_label" {
		for _, row := range result.Rows {
			if len(row) == 0 || !looksLikeDate(row[0]) {
				return false
			}
		}
		return true
	}
	return false
}

// looksLikeDate централизует проверку для последующего ветвления логики.
func looksLikeDate(value string) bool {
	text := strings.TrimSpace(value)
	if text == "" {
		return false
	}
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if _, err := time.Parse(layout, text); err == nil {
			return true
		}
	}
	return false
}

// chartValueIndex определяет колонку со значением метрики для карточек и подсказок.
func chartValueIndex(result shared.QueryResult) int {
	for index, column := range result.Columns {
		if strings.EqualFold(strings.TrimSpace(column), "metric_value") {
			return index
		}
	}
	if len(result.Columns) >= 3 {
		return 2
	}
	if len(result.Columns) >= 2 {
		return 1
	}
	return 0
}

// chartHighlights выполняет отдельный шаг окружающего сервисного сценария.
func chartHighlights(result shared.QueryResult) []string {
	if len(result.Rows) == 0 || len(result.Columns) < 2 {
		return nil
	}
	valueIndex := chartValueIndex(result)
	labelIndex := 0

	type point struct {
		label string
		value float64
	}

	points := make([]point, 0, len(result.Rows))
	total := 0.0
	for _, row := range result.Rows {
		if len(row) <= valueIndex || len(row) == 0 {
			continue
		}
		value := parseChartNumber(row[valueIndex])
		points = append(points, point{label: row[labelIndex], value: value})
		total += value
	}
	if len(points) == 0 {
		return nil
	}

	top := points[0]
	bottom := points[0]
	for _, item := range points[1:] {
		if item.value > top.value {
			top = item
		}
		if item.value < bottom.value {
			bottom = item
		}
	}

	average := total / float64(len(points))
	topShare := 0.0
	if total > 0 {
		topShare = top.value / total
	}

	return []string{
		fmt.Sprintf("Лидер %s: %s", top.label, trimNumeric(strconv.FormatFloat(top.value, 'f', 2, 64))),
		fmt.Sprintf("Минимум %s: %s", bottom.label, trimNumeric(strconv.FormatFloat(bottom.value, 'f', 2, 64))),
		fmt.Sprintf("Среднее: %s", trimNumeric(strconv.FormatFloat(average, 'f', 2, 64))),
		fmt.Sprintf("Доля лидера: %d%%", int(math.Round(topShare*100))),
	}
}

// parseChartNumber нормализует граничные значения перед дальнейшим использованием.
func parseChartNumber(value string) float64 {
	text := strings.ReplaceAll(strings.TrimSpace(value), " ", "")
	text = strings.ReplaceAll(text, ",", ".")
	number, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0
	}
	return number
}
