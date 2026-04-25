package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"drivee-self-service/internal/shared"
)

// executeQueryText координирует побочные эффекты выполнения и фиксирует результат.
func (app application) executeQueryText(ctx context.Context, queryText string) (shared.RunResponse, error) {
	payload := shared.QueryRequest{Text: queryText}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, app.queryURL+"/api/v1/query/run", bytes.NewReader(rawBody))
	if err != nil {
		return shared.RunResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.client.Do(req)
	if err != nil {
		return shared.RunResponse{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return shared.RunResponse{}, fmt.Errorf("%s", strings.TrimSpace(string(body)))
	}

	var runResp shared.RunResponse
	if err := json.Unmarshal(body, &runResp); err != nil {
		return shared.RunResponse{}, err
	}
	return runResp, nil
}

// saveRunSnapshot выполняет изменение с учётом владения и правил валидации.
func (app application) saveRunSnapshot(ctx context.Context, name, queryText string, run shared.RunResponse, source string, templateID *int64, ownerName, ownerDepartment string, isPublic bool, ownerUserID int64) (shared.SavedReport, error) {
	report := shared.SavedReport{
		Name:            name,
		QueryText:       queryText,
		SQLText:         run.SQL,
		Intent:          run.Intent,
		Preview:         run.Preview,
		Result:          run.Result,
		Provider:        run.Provider,
		Source:          source,
		OwnerName:       strings.TrimSpace(ownerName),
		OwnerDepartment: strings.TrimSpace(ownerDepartment),
		IsPublic:        isPublic,
		TemplateID:      templateID,
	}
	if report.IsPublic && report.OwnerDepartment == "" {
		return shared.SavedReport{}, fmt.Errorf("owner_department is required for public reports")
	}
	if report.OwnerName == "" {
		return shared.SavedReport{}, fmt.Errorf("report owner_name is required")
	}

	var templateValue any
	if templateID != nil && *templateID > 0 {
		templateValue = *templateID
	}
	var ownerUserValue any
	if ownerUserID > 0 {
		ownerUserValue = ownerUserID
	}

	err := app.db.QueryRow(ctx, `
		insert into app.saved_reports (
			name,
			query_text,
			sql_text,
			intent,
			preview_json,
			result_json,
			provider,
			source,
			owner_name,
			owner_department,
			is_public,
			template_id,
			owner_user_id
		)
		values ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8, $9, $10, $11, $12, $13)
		returning id, created_at, updated_at
	`, report.Name, report.QueryText, report.SQLText, shared.MustJSON(report.Intent), shared.MustJSON(report.Preview), shared.MustJSON(report.Result), report.Provider, source, report.OwnerName, report.OwnerDepartment, report.IsPublic, templateValue, ownerUserValue).
		Scan(&report.ID, &report.CreatedAt, &report.UpdatedAt)
	if err != nil {
		return shared.SavedReport{}, err
	}

	return report, nil
}

// fetchSavedReports загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchSavedReports(ctx context.Context, viewer templateViewer) ([]shared.SavedReport, error) {
	rows, err := app.db.Query(ctx, `
		select
			sr.id,
			sr.name,
			sr.query_text,
			sr.sql_text,
			sr.intent,
			coalesce(sr.preview_json, '{}'::jsonb),
			coalesce(sr.result_json, '{"columns":[],"rows":[],"count":0}'::jsonb),
			coalesce(sr.provider, ''),
			coalesce(sr.source, 'manual'),
			coalesce(sr.owner_name, ''),
			coalesce(sr.owner_department, ''),
			coalesce(sr.is_public, false),
			coalesce(sr.template_id, 0),
			coalesce(rt.name, ''),
			sr.created_at,
			sr.updated_at
		from app.saved_reports sr
		left join app.report_templates rt on rt.id = sr.template_id
		where
			($1 <> '' and sr.owner_name = $1)
			or sr.is_public = true
		order by sr.updated_at desc
	`, strings.TrimSpace(viewer.Name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	reports := make([]shared.SavedReport, 0)
	for rows.Next() {
		report, err := scanSavedReport(rows)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, rows.Err()
}

// fetchSavedReportByID загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchSavedReportByID(ctx context.Context, reportID int64, viewer templateViewer) (shared.SavedReport, error) {
	row := app.db.QueryRow(ctx, `
		select
			sr.id,
			sr.name,
			sr.query_text,
			sr.sql_text,
			sr.intent,
			coalesce(sr.preview_json, '{}'::jsonb),
			coalesce(sr.result_json, '{"columns":[],"rows":[],"count":0}'::jsonb),
			coalesce(sr.provider, ''),
			coalesce(sr.source, 'manual'),
			coalesce(sr.owner_name, ''),
			coalesce(sr.owner_department, ''),
			coalesce(sr.is_public, false),
			coalesce(sr.template_id, 0),
			coalesce(rt.name, ''),
			sr.created_at,
			sr.updated_at
		from app.saved_reports sr
		left join app.report_templates rt on rt.id = sr.template_id
		where sr.id = $1
			and (
				($2 <> '' and sr.owner_name = $2)
				or sr.is_public = true
			)
	`, reportID, strings.TrimSpace(viewer.Name))
	return scanSavedReport(row)
}

// scanSavedReport выполняет отдельный шаг окружающего сервисного сценария.
func scanSavedReport(scanner interface {
	Scan(dest ...any) error
}) (shared.SavedReport, error) {
	var report shared.SavedReport
	var intentRaw []byte
	var previewRaw []byte
	var resultRaw []byte
	var templateID int64

	err := scanner.Scan(
		&report.ID,
		&report.Name,
		&report.QueryText,
		&report.SQLText,
		&intentRaw,
		&previewRaw,
		&resultRaw,
		&report.Provider,
		&report.Source,
		&report.OwnerName,
		&report.OwnerDepartment,
		&report.IsPublic,
		&templateID,
		&report.TemplateName,
		&report.CreatedAt,
		&report.UpdatedAt,
	)
	if err != nil {
		return shared.SavedReport{}, err
	}

	_ = json.Unmarshal(intentRaw, &report.Intent)
	_ = json.Unmarshal(previewRaw, &report.Preview)
	_ = json.Unmarshal(resultRaw, &report.Result)
	if templateID > 0 {
		report.TemplateID = &templateID
	}
	return report, nil
}
