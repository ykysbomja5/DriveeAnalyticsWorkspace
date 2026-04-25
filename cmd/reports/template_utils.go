package main

import (
	"fmt"
	"os"
	"strings"

	"drivee-self-service/internal/shared"
)

// validateTemplateSchedule проверяет доменные ограничения до записи или выполнения.
func validateTemplateSchedule(schedule shared.ReportTemplateSchedule) error {
	if !schedule.Enabled {
		return nil
	}
	if schedule.DayOfWeek < 0 || schedule.DayOfWeek > 6 {
		return fmt.Errorf("schedule day_of_week must be between 0 and 6")
	}
	if schedule.Hour < 0 || schedule.Hour > 23 {
		return fmt.Errorf("schedule hour must be between 0 and 23")
	}
	if schedule.Minute < 0 || schedule.Minute > 59 {
		return fmt.Errorf("schedule minute must be between 0 and 59")
	}
	return nil
}

// nullableText выполняет отдельный шаг окружающего сервисного сценария.
func nullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

// nullableInt выполняет отдельный шаг окружающего сервисного сценария.
func nullableInt(enabled bool, value int) any {
	if !enabled {
		return nil
	}
	return value
}

// coalesceString нормализует граничные значения перед дальнейшим использованием.
func coalesceString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// getenv изолирует небольшой важный helper для общего сценария.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
