package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgtype"
)

// ensureTemplateOwnership подготавливает или проверяет нужное состояние БД и приложения.
func (app application) ensureTemplateOwnership(ctx context.Context, templateID int64, viewerName string) error {
	if strings.TrimSpace(viewerName) == "" {
		return fmt.Errorf("template owner name is required")
	}

	var ownerName string
	err := app.db.QueryRow(ctx, `
		select owner_name
		from app.report_templates
		where id = $1
	`, templateID).Scan(&ownerName)
	if err != nil {
		return fmt.Errorf("template not found")
	}
	if !strings.EqualFold(strings.TrimSpace(ownerName), strings.TrimSpace(viewerName)) {
		return fmt.Errorf("only the template owner can modify it")
	}
	return nil
}

// ensureReportOwnership подготавливает или проверяет нужное состояние БД и приложения.
func (app application) ensureReportOwnership(ctx context.Context, reportID int64, viewerName string) error {
	if strings.TrimSpace(viewerName) == "" {
		return fmt.Errorf("report owner name is required")
	}

	var ownerName string
	err := app.db.QueryRow(ctx, `
		select owner_name
		from app.saved_reports
		where id = $1
	`, reportID).Scan(&ownerName)
	if err != nil {
		return fmt.Errorf("report not found")
	}
	if !strings.EqualFold(strings.TrimSpace(ownerName), strings.TrimSpace(viewerName)) {
		return fmt.Errorf("only the report owner can modify it")
	}
	return nil
}

// updateReportSharing выполняет изменение с учётом владения и правил валидации.
func (app application) updateReportSharing(w http.ResponseWriter, r *http.Request, reportID int64) {
	viewer := viewerFromRequest(r)
	if err := app.ensureReportOwnership(r.Context(), reportID, viewer.Name); err != nil {
		status := http.StatusForbidden
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}

	var req shared.SharingSettingsRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.IsPublic && strings.TrimSpace(req.OwnerDepartment) == "" {
		req.OwnerDepartment = viewer.Department
	}
	if req.IsPublic && strings.TrimSpace(req.OwnerDepartment) == "" {
		shared.WriteError(w, http.StatusBadRequest, "owner_department is required for public reports")
		return
	}

	tag, err := app.db.Exec(r.Context(), `
		update app.saved_reports
		set
			is_public = $2,
			owner_department = $3,
			updated_at = now()
		where id = $1
	`, reportID, req.IsPublic, strings.TrimSpace(req.OwnerDepartment))
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	report, err := app.fetchSavedReportByID(r.Context(), reportID, viewer)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}
	shared.WriteJSON(w, http.StatusOK, report)
}

// upsertTemplate выполняет изменение с учётом владения и правил валидации.
func (app application) upsertTemplate(ctx context.Context, templateID int64, req shared.UpsertReportTemplateRequest) (shared.ReportTemplate, error) {
	if strings.TrimSpace(req.Name) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("template name is required")
	}
	if strings.TrimSpace(req.QueryText) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("template query_text is required")
	}
	if strings.TrimSpace(req.OwnerName) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("template owner_name is required")
	}
	if req.IsPublic && strings.TrimSpace(req.OwnerDepartment) == "" {
		return shared.ReportTemplate{}, fmt.Errorf("owner_department is required for public templates")
	}
	if err := validateTemplateSchedule(req.Schedule); err != nil {
		return shared.ReportTemplate{}, err
	}

	scheduleTimezone := coalesceString(req.Schedule.Timezone, app.location.String())
	var row interface {
		Scan(dest ...any) error
	}

	if templateID == 0 {
		row = app.db.QueryRow(ctx, `
			insert into app.report_templates (
				name,
				description,
				query_text,
				owner_name,
				owner_department,
				is_public,
				schedule_enabled,
				schedule_day_of_week,
				schedule_hour,
				schedule_minute,
				schedule_timezone
			)
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			returning id, name, description, query_text, owner_name, owner_department, is_public, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		`, strings.TrimSpace(req.Name), strings.TrimSpace(req.Description), strings.TrimSpace(req.QueryText), strings.TrimSpace(req.OwnerName), strings.TrimSpace(req.OwnerDepartment), req.IsPublic, req.Schedule.Enabled, nullableInt(req.Schedule.Enabled, req.Schedule.DayOfWeek), nullableInt(req.Schedule.Enabled, req.Schedule.Hour), nullableInt(req.Schedule.Enabled, req.Schedule.Minute), scheduleTimezone)
	} else {
		row = app.db.QueryRow(ctx, `
			update app.report_templates
			set
				name = $2,
				description = $3,
				query_text = $4,
				owner_name = $5,
				owner_department = $6,
				is_public = $7,
				schedule_enabled = $8,
				schedule_day_of_week = $9,
				schedule_hour = $10,
				schedule_minute = $11,
				schedule_timezone = $12,
				updated_at = now()
			where id = $1
			returning id, name, description, query_text, owner_name, owner_department, is_public, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		`, templateID, strings.TrimSpace(req.Name), strings.TrimSpace(req.Description), strings.TrimSpace(req.QueryText), strings.TrimSpace(req.OwnerName), strings.TrimSpace(req.OwnerDepartment), req.IsPublic, req.Schedule.Enabled, nullableInt(req.Schedule.Enabled, req.Schedule.DayOfWeek), nullableInt(req.Schedule.Enabled, req.Schedule.Hour), nullableInt(req.Schedule.Enabled, req.Schedule.Minute), scheduleTimezone)
	}

	record, err := scanTemplateRecord(row)
	if err != nil {
		if templateID > 0 {
			return shared.ReportTemplate{}, fmt.Errorf("template not found")
		}
		return shared.ReportTemplate{}, err
	}

	app.decorateTemplate(&record.ReportTemplate)
	return record.ReportTemplate, nil
}

// fetchTemplates загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchTemplates(ctx context.Context, viewer templateViewer) ([]shared.ReportTemplate, error) {
	rows, err := app.db.Query(ctx, `
		select id, name, description, query_text, owner_name, owner_department, is_public, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		from app.report_templates
		where
			($1 <> '' and owner_name = $1)
			or is_public = true
		order by
			case when owner_name = $1 then 0 else 1 end,
			is_public desc,
			updated_at desc,
			id desc
	`, strings.TrimSpace(viewer.Name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	templates := make([]shared.ReportTemplate, 0)
	for rows.Next() {
		record, err := scanTemplateRecord(rows)
		if err != nil {
			return nil, err
		}
		app.decorateTemplate(&record.ReportTemplate)
		templates = append(templates, record.ReportTemplate)
	}
	return templates, rows.Err()
}

// fetchTemplateByID загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchTemplateByID(ctx context.Context, templateID int64) (templateRecord, error) {
	row := app.db.QueryRow(ctx, `
		select id, name, description, query_text, owner_name, owner_department, is_public, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
		from app.report_templates
		where id = $1
	`, templateID)
	record, err := scanTemplateRecord(row)
	if err != nil {
		return templateRecord{}, err
	}
	app.decorateTemplate(&record.ReportTemplate)
	return record, nil
}

// scanTemplateRecord выполняет отдельный шаг окружающего сервисного сценария.
func scanTemplateRecord(scanner interface {
	Scan(dest ...any) error
}) (templateRecord, error) {
	var record templateRecord
	var enabled bool
	var timezone string
	var day, hour, minute pgtype.Int4
	var lastRun pgtype.Timestamptz
	var lastScheduled pgtype.Timestamptz

	err := scanner.Scan(
		&record.ID,
		&record.Name,
		&record.Description,
		&record.QueryText,
		&record.OwnerName,
		&record.OwnerDepartment,
		&record.IsPublic,
		&enabled,
		&day,
		&hour,
		&minute,
		&timezone,
		&lastRun,
		&lastScheduled,
		&record.LastStatus,
		&record.LastErrorText,
		&record.LastResultCount,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return templateRecord{}, err
	}

	record.Schedule.Enabled = enabled
	record.Schedule.Timezone = timezone
	if day.Valid {
		record.Schedule.DayOfWeek = int(day.Int32)
	}
	if hour.Valid {
		record.Schedule.Hour = int(hour.Int32)
	}
	if minute.Valid {
		record.Schedule.Minute = int(minute.Int32)
	}
	if lastRun.Valid {
		value := lastRun.Time
		record.LastRunAt = &value
	}
	if lastScheduled.Valid {
		value := lastScheduled.Time
		record.lastScheduledFor = &value
	}
	return record, nil
}

// decorateTemplate выполняет отдельный шаг окружающего сервисного сценария.
func (app application) decorateTemplate(template *shared.ReportTemplate) {
	template.Schedule.Label = humanScheduleLabel(template.Schedule)
	if nextRun := nextRunForSchedule(template.Schedule, time.Now().In(app.location), app.location); !nextRun.IsZero() {
		template.Schedule.NextRun = nextRun.Format(time.RFC3339)
	}
}

// logExecution координирует побочные эффекты выполнения и фиксирует результат.
func (app application) logExecution(ctx context.Context, reportID int64, status string, rowCount int, errorText string) {
	_, _ = app.db.Exec(ctx, `
		insert into app.report_runs (report_id, status, row_count, error_text)
		values ($1, $2, $3, $4)
	`, reportID, status, rowCount, nullableText(errorText))
}
