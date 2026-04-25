package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

// application объединяет зависимости сервиса для HTTP-обработчиков.
type application struct {
	db       *pgxpool.Pool
	client   *http.Client
	queryURL string
	location *time.Location
}

// templateRecord хранит данные из БД вместе со служебными полями сервиса.
type templateRecord struct {
	shared.ReportTemplate
	lastScheduledFor *time.Time
}

// templateViewer передаёт контекст пользователя для проверки доступа.
type templateViewer struct {
	ID         int64
	Name       string
	Department string
	Role       string
}

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	port := getenv("PORT", getenv("REPORTS_PORT", "8083"))
	dsn := os.Getenv("PG_DSN")
	if strings.TrimSpace(dsn) == "" {
		log.Fatal("PG_DSN is required for reports service")
	}

	location := time.Local
	if envTZ := strings.TrimSpace(os.Getenv("APP_TIMEZONE")); envTZ != "" {
		if loaded, err := time.LoadLocation(envTZ); err == nil {
			location = loaded
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer pool.Close()

	if err := shared.EnsureIdentitySchema(ctx, pool); err != nil {
		log.Fatalf("failed to prepare identity schema: %v", err)
	}
	if err := ensureDatabaseObjects(ctx, pool); err != nil {
		log.Fatalf("failed to prepare reports schema: %v", err)
	}

	app := application{
		db:       pool,
		client:   &http.Client{Timeout: 25 * time.Second},
		queryURL: getenv("QUERY_SERVICE_URL", "http://localhost:8081"),
		location: location,
	}
	go app.startTemplateScheduler()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/reports/export", app.handleDirectExport)
	mux.HandleFunc("/api/v1/reports/templates", app.handleTemplates)
	mux.HandleFunc("/api/v1/reports/templates/", app.handleTemplateActions)
	mux.HandleFunc("/api/v1/reports", app.handleReports)
	mux.HandleFunc("/api/v1/reports/", app.handleReportActions)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "reports"})
	})

	log.Printf("reports listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// ensureDatabaseObjects подготавливает или проверяет нужное состояние БД и приложения.
func ensureDatabaseObjects(ctx context.Context, db *pgxpool.Pool) error {
	statements := []string{
		`create table if not exists app.report_templates (
			id bigserial primary key,
			name text not null,
			description text not null default '',
			query_text text not null,
			owner_name text not null default '',
			owner_department text not null default '',
			is_public boolean not null default false,
			schedule_enabled boolean not null default false,
			schedule_day_of_week integer,
			schedule_hour integer,
			schedule_minute integer,
			schedule_timezone text not null default 'Europe/Moscow',
			last_run_at timestamptz,
			last_scheduled_for timestamptz,
			last_status text not null default 'idle',
			last_error_text text,
			last_result_count integer not null default 0,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now(),
			constraint report_templates_schedule_day check (schedule_day_of_week is null or schedule_day_of_week between 0 and 6),
			constraint report_templates_schedule_hour check (schedule_hour is null or schedule_hour between 0 and 23),
			constraint report_templates_schedule_minute check (schedule_minute is null or schedule_minute between 0 and 59)
		)`,
		`alter table app.saved_reports add column if not exists preview_json jsonb`,
		`alter table app.saved_reports add column if not exists result_json jsonb`,
		`alter table app.saved_reports add column if not exists provider text`,
		`alter table app.saved_reports add column if not exists source text not null default 'manual'`,
		`alter table app.saved_reports add column if not exists owner_name text not null default ''`,
		`alter table app.saved_reports add column if not exists owner_department text not null default ''`,
		`alter table app.saved_reports add column if not exists is_public boolean not null default false`,
		`alter table app.saved_reports add column if not exists template_id bigint`,
		`alter table app.saved_reports add column if not exists owner_user_id bigint references app.users(id) on delete set null`,
		`alter table app.report_templates add column if not exists owner_name text not null default ''`,
		`alter table app.report_templates add column if not exists owner_department text not null default ''`,
		`alter table app.report_templates add column if not exists is_public boolean not null default false`,
		`alter table app.report_templates add column if not exists owner_user_id bigint references app.users(id) on delete set null`,
		`create index if not exists idx_saved_reports_template_id on app.saved_reports (template_id, updated_at desc)`,
		`create index if not exists idx_saved_reports_owner on app.saved_reports (owner_name, updated_at desc)`,
		`create index if not exists idx_saved_reports_owner_user on app.saved_reports (owner_user_id, updated_at desc)`,
		`create index if not exists idx_saved_reports_public_department on app.saved_reports (is_public, owner_department, updated_at desc)`,
		`create index if not exists idx_report_templates_schedule on app.report_templates (schedule_enabled, schedule_day_of_week, schedule_hour, schedule_minute)`,
		`create index if not exists idx_report_templates_owner on app.report_templates (owner_name, updated_at desc)`,
		`create index if not exists idx_report_templates_owner_user on app.report_templates (owner_user_id, updated_at desc)`,
		`create index if not exists idx_report_templates_public_department on app.report_templates (is_public, owner_department, updated_at desc)`,
		`do $$
		begin
			if not exists (
				select 1
				from pg_constraint
				where conname = 'saved_reports_template_id_fkey'
			) then
				alter table app.saved_reports
					add constraint saved_reports_template_id_fkey
					foreign key (template_id) references app.report_templates(id) on delete set null;
			end if;
		end $$`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// handleReports проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleReports(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	switch r.Method {
	case http.MethodPost:
		app.saveReport(w, r)
	case http.MethodGet:
		app.listReports(w, r)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleTemplates проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		app.listTemplates(w, r)
	case http.MethodPost:
		app.createTemplate(w, r)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// viewerFromRequest выполняет отдельный шаг окружающего сервисного сценария.
func viewerFromRequest(r *http.Request) templateViewer {
	name := decodeViewerHeader(r.Header.Get("X-Drivee-User"))
	if name == "" {
		name = "Локальный пользователь"
	}
	userID, _ := strconv.ParseInt(strings.TrimSpace(r.Header.Get("X-Drivee-User-Id")), 10, 64)
	return templateViewer{
		ID:         userID,
		Name:       name,
		Department: decodeViewerHeader(r.Header.Get("X-Drivee-Department")),
		Role:       strings.TrimSpace(r.Header.Get("X-Drivee-Role")),
	}
}

// decodeViewerHeader нормализует граничные значения перед дальнейшим использованием.
func decodeViewerHeader(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	decoded, err := url.QueryUnescape(value)
	if err != nil {
		return value
	}
	return strings.TrimSpace(decoded)
}

// normalizeTemplateRequest нормализует граничные значения перед дальнейшим использованием.
func normalizeTemplateRequest(req *shared.UpsertReportTemplateRequest, viewer templateViewer) {
	if strings.TrimSpace(req.OwnerName) == "" {
		req.OwnerName = viewer.Name
	}
	if strings.TrimSpace(req.OwnerDepartment) == "" {
		req.OwnerDepartment = viewer.Department
	}
}
