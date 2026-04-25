package main

import (
	"testing"
	"time"

	"drivee-self-service/internal/shared"
)

func TestHumanScheduleLabel(t *testing.T) {
	schedule := shared.ReportTemplateSchedule{
		Enabled:   true,
		DayOfWeek: 1,
		Hour:      9,
		Minute:    5,
	}

	got := humanScheduleLabel(schedule)
	want := "Каждый понедельник в 09:05"
	if got != want {
		t.Fatalf("humanScheduleLabel() = %q, want %q", got, want)
	}
}

func TestNextRunForSchedule(t *testing.T) {
	location := time.FixedZone("MSK", 3*60*60)
	now := time.Date(2026, time.April, 23, 16, 14, 0, 0, location)
	schedule := shared.ReportTemplateSchedule{
		Enabled:   true,
		DayOfWeek: int(time.Wednesday),
		Hour:      10,
		Minute:    30,
		Timezone:  location.String(),
	}

	got := nextRunForSchedule(schedule, now, location)
	want := time.Date(2026, time.April, 29, 10, 30, 0, 0, location)
	if !got.Equal(want) {
		t.Fatalf("nextRunForSchedule() = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestDueRunForScheduleReturnsPreviousSlot(t *testing.T) {
	location := time.FixedZone("MSK", 3*60*60)
	now := time.Date(2026, time.April, 23, 16, 14, 0, 0, location)
	schedule := shared.ReportTemplateSchedule{
		Enabled:   true,
		DayOfWeek: int(time.Thursday),
		Hour:      15,
		Minute:    0,
		Timezone:  location.String(),
	}

	got := dueRunForSchedule(schedule, now, location)
	want := time.Date(2026, time.April, 23, 15, 0, 0, 0, location)
	if !got.Equal(want) {
		t.Fatalf("dueRunForSchedule() = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}
