package main

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
)

// handleReportActions проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleReportActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/reports/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		shared.WriteError(w, http.StatusNotFound, "report route not found")
		return
	}

	reportID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid report id")
		return
	}

	if len(parts) == 1 {
		if r.Method == http.MethodDelete {
			app.deleteReport(w, r, reportID)
			return
		}
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if len(parts) != 2 {
		shared.WriteError(w, http.StatusNotFound, "report route not found")
		return
	}

	switch {
	case parts[1] == "run" && r.Method == http.MethodPost:
		app.runReport(w, r, reportID)
	case parts[1] == "export" && r.Method == http.MethodGet:
		app.exportSavedReport(w, r, reportID)
	case parts[1] == "sharing" && r.Method == http.MethodPut:
		app.updateReportSharing(w, r, reportID)
	default:
		shared.WriteError(w, http.StatusNotFound, "report route not found")
	}
}

// handleDirectExport проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleDirectExport(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "pdf" && format != "docx" {
		shared.WriteError(w, http.StatusBadRequest, "format must be pdf or docx")
		return
	}

	var req shared.ExportReportRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		req.Name = "Аналитический отчет"
	}

	payload := exportPayload{
		Name:      req.Name,
		QueryText: req.QueryText,
		Run:       req.Run,
		CreatedAt: time.Now().In(app.location),
	}
	app.writeExport(w, payload, format)
}

// saveReport выполняет изменение с учётом владения и правил валидации.
func (app application) saveReport(w http.ResponseWriter, r *http.Request) {
	var req shared.SaveReportRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Name) == "" || strings.TrimSpace(req.QueryText) == "" || strings.TrimSpace(req.SQLText) == "" {
		shared.WriteError(w, http.StatusBadRequest, "name, query_text and sql_text are required")
		return
	}
	viewer := viewerFromRequest(r)
	if strings.TrimSpace(req.OwnerName) == "" {
		req.OwnerName = viewer.Name
	}
	if strings.TrimSpace(req.OwnerDepartment) == "" {
		req.OwnerDepartment = viewer.Department
	}
	if strings.TrimSpace(req.OwnerName) == "" {
		shared.WriteError(w, http.StatusBadRequest, "report owner_name is required")
		return
	}
	if req.IsPublic && strings.TrimSpace(req.OwnerDepartment) == "" {
		shared.WriteError(w, http.StatusBadRequest, "owner_department is required for public reports")
		return
	}

	viewerID := viewer.ID
	report, err := app.saveRunSnapshot(r.Context(), req.Name, req.QueryText, shared.RunResponse{
		Intent:   req.Intent,
		Preview:  req.Preview,
		SQL:      req.SQLText,
		Result:   req.Result,
		Provider: req.Provider,
	}, coalesceString(req.Source, "manual"), req.TemplateID, req.OwnerName, req.OwnerDepartment, req.IsPublic, viewerID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusCreated, report)
}

// listReports загружает записи, доступные текущему пользователю или сценарию.
func (app application) listReports(w http.ResponseWriter, r *http.Request) {
	reports, err := app.fetchSavedReports(r.Context(), viewerFromRequest(r))
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, reports)
}

// runReport координирует побочные эффекты выполнения и фиксирует результат.
func (app application) runReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	report, err := app.fetchSavedReportByID(r.Context(), reportID, viewerFromRequest(r))
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	started := time.Now()
	runResp, err := app.executeQueryText(r.Context(), report.QueryText)
	if err != nil {
		app.logExecution(r.Context(), reportID, "failed", 0, err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	app.logExecution(r.Context(), reportID, "ok", runResp.Result.Count, "")
	log.Printf("report %d executed in %s", reportID, time.Since(started))
	shared.WriteJSON(w, http.StatusOK, runResp)
}

// deleteReport выполняет изменение с учётом владения и правил валидации.
func (app application) deleteReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	if err := app.ensureReportOwnership(r.Context(), reportID, viewerFromRequest(r).Name); err != nil {
		status := http.StatusForbidden
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}
	tag, err := app.db.Exec(r.Context(), `delete from app.saved_reports where id = $1`, reportID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// exportSavedReport выполняет отдельный шаг окружающего сервисного сценария.
func (app application) exportSavedReport(w http.ResponseWriter, r *http.Request, reportID int64) {
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "pdf" && format != "docx" {
		shared.WriteError(w, http.StatusBadRequest, "format must be pdf or docx")
		return
	}

	report, err := app.fetchSavedReportByID(r.Context(), reportID, viewerFromRequest(r))
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "report not found")
		return
	}

	run := shared.RunResponse{
		Intent:   report.Intent,
		Preview:  report.Preview,
		SQL:      report.SQLText,
		Result:   report.Result,
		Provider: report.Provider,
	}
	if len(run.Result.Columns) == 0 && strings.TrimSpace(report.QueryText) != "" {
		run, err = app.executeQueryText(r.Context(), report.QueryText)
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
	}

	payload := exportPayload{
		Name:      report.Name,
		QueryText: report.QueryText,
		Run:       run,
		CreatedAt: time.Now().In(app.location),
	}
	app.writeExport(w, payload, format)
}

// listTemplates загружает записи, доступные текущему пользователю или сценарию.
func (app application) listTemplates(w http.ResponseWriter, r *http.Request) {
	templates, err := app.fetchTemplates(r.Context(), viewerFromRequest(r))
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, templates)
}

// createTemplate выполняет изменение с учётом владения и правил валидации.
func (app application) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req shared.UpsertReportTemplateRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	viewer := viewerFromRequest(r)
	normalizeTemplateRequest(&req, viewer)

	template, err := app.upsertTemplate(r.Context(), 0, req)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if viewer.ID > 0 {
		_, _ = app.db.Exec(r.Context(), `update app.report_templates set owner_user_id = $2 where id = $1`, template.ID, viewer.ID)
	}
	shared.WriteJSON(w, http.StatusCreated, template)
}

// updateTemplate выполняет изменение с учётом владения и правил валидации.
func (app application) updateTemplate(w http.ResponseWriter, r *http.Request, templateID int64) {
	var req shared.UpsertReportTemplateRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	viewer := viewerFromRequest(r)
	normalizeTemplateRequest(&req, viewer)
	if err := app.ensureTemplateOwnership(r.Context(), templateID, viewer.Name); err != nil {
		status := http.StatusForbidden
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}

	template, err := app.upsertTemplate(r.Context(), templateID, req)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}
	if viewer.ID > 0 {
		_, _ = app.db.Exec(r.Context(), `update app.report_templates set owner_user_id = $2 where id = $1`, template.ID, viewer.ID)
	}
	shared.WriteJSON(w, http.StatusOK, template)
}

// deleteTemplate выполняет изменение с учётом владения и правил валидации.
func (app application) deleteTemplate(w http.ResponseWriter, r *http.Request, templateID int64) {
	if err := app.ensureTemplateOwnership(r.Context(), templateID, viewerFromRequest(r).Name); err != nil {
		status := http.StatusForbidden
		if strings.Contains(err.Error(), "not found") {
			status = http.StatusNotFound
		}
		shared.WriteError(w, status, err.Error())
		return
	}

	tag, err := app.db.Exec(r.Context(), `delete from app.report_templates where id = $1`, templateID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	if tag.RowsAffected() == 0 {
		shared.WriteError(w, http.StatusNotFound, "template not found")
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

// updateTemplateSharing выполняет изменение с учётом владения и правил валидации.
func (app application) updateTemplateSharing(w http.ResponseWriter, r *http.Request, templateID int64) {
	viewer := viewerFromRequest(r)
	if err := app.ensureTemplateOwnership(r.Context(), templateID, viewer.Name); err != nil {
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
		shared.WriteError(w, http.StatusBadRequest, "owner_department is required for public templates")
		return
	}

	row := app.db.QueryRow(r.Context(), `
		update app.report_templates
		set
			is_public = $2,
			owner_department = $3,
			updated_at = now()
		where id = $1
		returning id, name, description, query_text, owner_name, owner_department, is_public, schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute, schedule_timezone, last_run_at, last_scheduled_for, last_status, coalesce(last_error_text, ''), last_result_count, created_at, updated_at
	`, templateID, req.IsPublic, strings.TrimSpace(req.OwnerDepartment))

	record, err := scanTemplateRecord(row)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "template not found")
		return
	}
	app.decorateTemplate(&record.ReportTemplate)
	shared.WriteJSON(w, http.StatusOK, record.ReportTemplate)
}

// runTemplateNow координирует побочные эффекты выполнения и фиксирует результат.
func (app application) runTemplateNow(w http.ResponseWriter, r *http.Request, templateID int64) {
	template, err := app.fetchTemplateByID(r.Context(), templateID)
	if err != nil {
		shared.WriteError(w, http.StatusNotFound, "template not found")
		return
	}
	if !template.IsPublic && !strings.EqualFold(strings.TrimSpace(template.OwnerName), strings.TrimSpace(viewerFromRequest(r).Name)) {
		shared.WriteError(w, http.StatusForbidden, "template is private")
		return
	}

	viewer := viewerFromRequest(r)
	ownerName := viewer.Name
	if strings.TrimSpace(ownerName) == "" {
		ownerName = template.OwnerName
	}
	ownerDepartment := viewer.Department
	if strings.TrimSpace(ownerDepartment) == "" {
		ownerDepartment = template.OwnerDepartment
	}

	app.markTemplateManualRunStarted(r.Context(), templateID)
	runResp, savedReport, err := app.executeTemplate(r.Context(), template, "template-manual", false, ownerName, ownerDepartment, template.IsPublic)
	if err != nil {
		app.markTemplateManualRunFailed(r.Context(), templateID, err.Error())
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	app.markTemplateManualRunSucceeded(r.Context(), templateID, runResp.Result.Count)

	shared.WriteJSON(w, http.StatusOK, map[string]any{
		"run":    runResp,
		"report": savedReport,
	})
}
