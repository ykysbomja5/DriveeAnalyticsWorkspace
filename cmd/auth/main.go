package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// application объединяет зависимости сервиса для HTTP-обработчиков.
type application struct {
	db *pgxpool.Pool
}

// authResponse описывает JSON-ответ, возвращаемый вызывающей стороне.
type authResponse struct {
	Token     string          `json:"token"`
	ExpiresAt time.Time       `json:"expires_at"`
	User      shared.AuthUser `json:"user"`
}

// registerRequest описывает JSON-запрос на границе API.
type registerRequest struct {
	Email          string `json:"email"`
	Password       string `json:"password"`
	FullName       string `json:"full_name"`
	DepartmentName string `json:"department_name"`
}

// loginRequest описывает JSON-запрос на границе API.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// updateUserRequest описывает JSON-запрос на границе API.
type updateUserRequest struct {
	FullName       string `json:"full_name"`
	Role           string `json:"role"`
	DepartmentName string `json:"department_name"`
	IsActive       *bool  `json:"is_active"`
}

// grantDepartmentRequest описывает JSON-запрос на границе API.
type grantDepartmentRequest struct {
	UserID       int64  `json:"user_id"`
	DepartmentID int64  `json:"department_id"`
	Department   string `json:"department,omitempty"`
}

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}
	dsn := strings.TrimSpace(os.Getenv("PG_DSN"))
	if dsn == "" {
		log.Fatal("PG_DSN is required for auth service")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := shared.OpenPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect postgres: %v", err)
	}
	defer pool.Close()
	if err := shared.EnsureIdentitySchema(ctx, pool); err != nil {
		log.Fatalf("failed to prepare auth schema: %v", err)
	}
	if err := ensureBootstrapRoot(ctx, pool); err != nil {
		log.Fatalf("failed to prepare root user: %v", err)
	}

	app := application{db: pool}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/register", app.handleRegister)
	mux.HandleFunc("/api/v1/auth/login", app.handleLogin)
	mux.HandleFunc("/api/v1/auth/logout", app.handleLogout)
	mux.HandleFunc("/api/v1/auth/me", app.handleMe)
	mux.HandleFunc("/api/v1/auth/users", app.handleUsers)
	mux.HandleFunc("/api/v1/auth/users/", app.handleUserActions)
	mux.HandleFunc("/api/v1/auth/users/pending", app.handlePendingUsers)
	mux.HandleFunc("/api/v1/auth/users/approve", app.handleApproveUser)
	mux.HandleFunc("/api/v1/auth/departments", app.handleDepartments)
	mux.HandleFunc("/api/v1/auth/department-access", app.handleDepartmentAccess)
	mux.HandleFunc("/internal/auth/validate", app.handleValidate)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "auth"})
	})

	port := getenv("PORT", getenv("AUTH_PORT", "8085"))
	log.Printf("auth listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// handleRegister проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleRegister(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req registerRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	user, err := app.createUser(r.Context(), req.Email, req.Password, req.FullName, shared.RoleUser, req.DepartmentName, true)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusCreated, map[string]interface{}{
		"message": "Регистрация успешна. Ожидайте подтверждения от администратора.",
		"user":    user,
	})
}

// handleLogin проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleLogin(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req loginRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	user, hash, err := app.userWithPassword(r.Context(), req.Email)
	if err != nil || !shared.VerifyPassword(req.Password, hash) {
		shared.WriteError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	if !user.IsApproved {
		shared.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
			"pending": true,
			"message": "Ваш аккаунт ожидает подтверждения администратора.",
			"user":    user,
		})
		return
	}
	app.writeSession(w, r.Context(), user, http.StatusOK)
}

// handleLogout проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleLogout(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	token := shared.BearerToken(r)
	if token != "" {
		_, _ = app.db.Exec(r.Context(), `delete from app.user_sessions where token_hash = $1`, shared.TokenHash(token))
	}
	shared.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleMe проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleMe(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, user)
}

// handleValidate проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, user)
}

// handleUsers проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleUsers(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	user, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	switch r.Method {
	case http.MethodGet:
		_ = user
		users, err := app.listUsers(r.Context())
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusOK, users)
	case http.MethodPost:
		if user.Role != shared.RoleRoot {
			shared.WriteError(w, http.StatusForbidden, "only root can create managed accounts")
			return
		}
		var req registerRequest
		if err := shared.DecodeJSON(r, &req); err != nil {
			shared.WriteError(w, http.StatusBadRequest, "invalid json")
			return
		}
		created, err := app.createUser(r.Context(), req.Email, req.Password, req.FullName, shared.RoleUser, req.DepartmentName, true)
		if err != nil {
			shared.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusCreated, created)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleUserActions проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleUserActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	current, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if current.Role != shared.RoleRoot {
		shared.WriteError(w, http.StatusForbidden, "only root can change user roles")
		return
	}
	if r.Method != http.MethodPatch {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id, err := strconv.ParseInt(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/auth/users/"), "/"), 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	var req updateUserRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	updated, err := app.updateUser(r.Context(), id, req)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, updated)
}

// handleDepartments проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleDepartments(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	if _, err := shared.RequireUser(r.Context(), app.db, r); err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if r.Method != http.MethodGet {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rows, err := app.db.Query(r.Context(), `select id, name from app.departments order by name`)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer rows.Close()
	type department struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	items := []department{}
	for rows.Next() {
		var item department
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		items = append(items, item)
	}
	shared.WriteJSON(w, http.StatusOK, items)
}

// handleDepartmentAccess проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleDepartmentAccess(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	current, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if current.Role != shared.RoleRoot && current.Role != shared.RoleManager {
		shared.WriteError(w, http.StatusForbidden, "not enough permissions")
		return
	}
	switch r.Method {
	case http.MethodGet:
		access, err := app.listDepartmentAccess(r.Context())
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusOK, access)
	case http.MethodPost:
		var req grantDepartmentRequest
		if err := shared.DecodeJSON(r, &req); err != nil {
			shared.WriteError(w, http.StatusBadRequest, "invalid json")
			return
		}
		departmentID := req.DepartmentID
		if departmentID == 0 && strings.TrimSpace(req.Department) != "" {
			departmentID, err = app.departmentID(r.Context(), req.Department)
			if err != nil {
				shared.WriteError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		if departmentID == 0 || req.UserID == 0 {
			shared.WriteError(w, http.StatusBadRequest, "user_id and department_id are required")
			return
		}
		if current.Role == shared.RoleManager && current.DepartmentID != departmentID {
			shared.WriteError(w, http.StatusForbidden, "manager can grant access only to own department")
			return
		}
		_, err := app.db.Exec(r.Context(), `
			insert into app.department_access (user_id, department_id, granted_by)
			values ($1, $2, $3)
			on conflict (user_id, department_id) do update set granted_by = excluded.granted_by
		`, req.UserID, departmentID, current.ID)
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusCreated, map[string]bool{"ok": true})
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handlePendingUsers проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handlePendingUsers(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	current, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if current.Role != shared.RoleRoot && current.Role != shared.RoleManager {
		shared.WriteError(w, http.StatusForbidden, "only admins can view pending users")
		return
	}
	if r.Method != http.MethodGet {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	pending, err := app.listPendingUsers(r.Context())
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, pending)
}

// handleApproveUser проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleApproveUser(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	current, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if current.Role != shared.RoleRoot && current.Role != shared.RoleManager {
		shared.WriteError(w, http.StatusForbidden, "only admins can approve users")
		return
	}
	if r.Method != http.MethodPost {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		UserID  int64 `json:"user_id"`
		Approve bool  `json:"approve"`
	}
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.UserID == 0 {
		shared.WriteError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	err = app.approveUser(r.Context(), req.UserID, req.Approve)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// createUser выполняет изменение с учётом владения и правил валидации.
func (app application) createUser(ctx context.Context, email, password, fullName, role, departmentName string, active bool) (shared.AuthUser, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	fullName = strings.TrimSpace(fullName)
	if email == "" || password == "" || fullName == "" {
		return shared.AuthUser{}, fmt.Errorf("email, password and full_name are required")
	}
	if role == "" {
		role = shared.RoleUser
	}
	if !validRole(role) {
		return shared.AuthUser{}, fmt.Errorf("invalid role")
	}
	hash, err := shared.HashPassword(password)
	if err != nil {
		return shared.AuthUser{}, err
	}
	var departmentID any
	if strings.TrimSpace(departmentName) != "" {
		id, err := app.departmentID(ctx, departmentName)
		if err != nil {
			return shared.AuthUser{}, err
		}
		departmentID = id
	}
	row := app.db.QueryRow(ctx, `
		insert into app.users (email, password_hash, full_name, role, department_id, is_active)
		values ($1, $2, $3, $4, $5, $6)
		returning id
	`, email, hash, fullName, role, departmentID, active)
	var id int64
	if err := row.Scan(&id); err != nil {
		return shared.AuthUser{}, err
	}
	return app.fetchUserByID(ctx, id)
}

// updateUser выполняет изменение с учётом владения и правил валидации.
func (app application) updateUser(ctx context.Context, userID int64, req updateUserRequest) (shared.AuthUser, error) {
	if req.Role != "" && !validRole(req.Role) {
		return shared.AuthUser{}, fmt.Errorf("invalid role")
	}
	existing, err := app.fetchUserByID(ctx, userID)
	if err != nil {
		return shared.AuthUser{}, err
	}
	fullName := existing.FullName
	if strings.TrimSpace(req.FullName) != "" {
		fullName = strings.TrimSpace(req.FullName)
	}
	role := existing.Role
	if strings.TrimSpace(req.Role) != "" {
		role = strings.TrimSpace(req.Role)
	}
	var departmentID any
	if strings.TrimSpace(req.DepartmentName) != "" {
		id, err := app.departmentID(ctx, req.DepartmentName)
		if err != nil {
			return shared.AuthUser{}, err
		}
		departmentID = id
	} else if existing.DepartmentID > 0 {
		departmentID = existing.DepartmentID
	}
	active := true
	if req.IsActive != nil {
		active = *req.IsActive
	} else {
		active = true
	}
	_, err = app.db.Exec(ctx, `
		update app.users
		set full_name = $2, role = $3, department_id = $4, is_active = $5, updated_at = now()
		where id = $1
	`, userID, fullName, role, departmentID, active)
	if err != nil {
		return shared.AuthUser{}, err
	}
	return app.fetchUserByID(ctx, userID)
}

// listUsers загружает записи, доступные текущему пользователю или сценарию.
func (app application) listUsers(ctx context.Context) ([]shared.AuthUser, error) {
	rows, err := app.db.Query(ctx, `
		select u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, ''), u.is_approved
		from app.users u
		left join app.departments d on d.id = u.department_id
		order by u.full_name, u.email
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := []shared.AuthUser{}
	for rows.Next() {
		var user shared.AuthUser
		if err := rows.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName, &user.IsApproved); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// listPendingUsers загружает записи, доступные текущему пользователю или сценарию.
func (app application) listPendingUsers(ctx context.Context) ([]shared.AuthUser, error) {
	rows, err := app.db.Query(ctx, `
		select u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, ''), u.is_approved
		from app.users u
		left join app.departments d on d.id = u.department_id
		where u.is_approved = false
		order by u.created_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := []shared.AuthUser{}
	for rows.Next() {
		var user shared.AuthUser
		if err := rows.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName, &user.IsApproved); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// approveUser выполняет изменение с учётом владения и правил валидации.
func (app application) approveUser(ctx context.Context, userID int64, approve bool) error {
	_, err := app.db.Exec(ctx, `
		update app.users
		set is_approved = $2,
			is_active = case when $2 then true else is_active end,
			updated_at = now()
		where id = $1
	`, userID, approve)
	return err
}

// listDepartmentAccess загружает записи, доступные текущему пользователю или сценарию.
func (app application) listDepartmentAccess(ctx context.Context) ([]map[string]any, error) {
	rows, err := app.db.Query(ctx, `
		select
			da.id,
			da.user_id,
			u.full_name,
			u.email,
			da.department_id,
			d.name,
			coalesce(g.full_name, ''),
			da.created_at
		from app.department_access da
		join app.users u on u.id = da.user_id
		join app.departments d on d.id = da.department_id
		left join app.users g on g.id = da.granted_by
		order by da.created_at desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, userID, departmentID int64
		var fullName, email, department, grantedBy string
		var createdAt time.Time
		if err := rows.Scan(&id, &userID, &fullName, &email, &departmentID, &department, &grantedBy, &createdAt); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"id":              id,
			"user_id":         userID,
			"full_name":       fullName,
			"email":           email,
			"department_id":   departmentID,
			"department_name": department,
			"granted_by":      grantedBy,
			"created_at":      createdAt,
		})
	}
	return result, rows.Err()
}

// fetchUserByID загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchUserByID(ctx context.Context, id int64) (shared.AuthUser, error) {
	row := app.db.QueryRow(ctx, `
		select u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, ''), u.is_approved
		from app.users u
		left join app.departments d on d.id = u.department_id
		where u.id = $1
	`, id)
	var user shared.AuthUser
	if err := row.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName, &user.IsApproved); err != nil {
		return shared.AuthUser{}, err
	}
	return user, nil
}

// userWithPassword выполняет отдельный шаг окружающего сервисного сценария.
func (app application) userWithPassword(ctx context.Context, email string) (shared.AuthUser, string, error) {
	row := app.db.QueryRow(ctx, `
		select u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, ''), u.password_hash, u.is_approved
		from app.users u
		left join app.departments d on d.id = u.department_id
		where lower(u.email) = lower($1) and u.is_active = true
	`, strings.TrimSpace(email))
	var user shared.AuthUser
	var hash string
	if err := row.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName, &hash, &user.IsApproved); err != nil {
		return shared.AuthUser{}, "", err
	}
	return user, hash, nil
}

// departmentID выполняет отдельный шаг окружающего сервисного сценария.
func (app application) departmentID(ctx context.Context, name string) (int64, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, fmt.Errorf("department name is required")
	}
	var id int64
	err := app.db.QueryRow(ctx, `
		insert into app.departments (name)
		values ($1)
		on conflict (name) do update set name = excluded.name
		returning id
	`, name).Scan(&id)
	return id, err
}

// writeSession сериализует подготовленные данные в нужный формат ответа.
func (app application) writeSession(w http.ResponseWriter, ctx context.Context, user shared.AuthUser, status int) {
	token, hash, err := shared.NewToken()
	if err != nil {
		shared.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	expiresAt := time.Now().Add(shared.SessionTTL)
	if _, err := app.db.Exec(ctx, `
		insert into app.user_sessions (user_id, token_hash, expires_at)
		values ($1, $2, $3)
	`, user.ID, hash, expiresAt); err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, status, authResponse{Token: token, ExpiresAt: expiresAt, User: user})
}

// ensureBootstrapRoot подготавливает или проверяет нужное состояние БД и приложения.
func ensureBootstrapRoot(ctx context.Context, db *pgxpool.Pool) error {
	email := getenv("ROOT_EMAIL", "root@drivee.local")
	password := getenv("ROOT_PASSWORD", "ChangeMe123!")
	fullName := getenv("ROOT_FULL_NAME", "Drivee Root")
	departmentName := getenv("ROOT_DEPARTMENT", "Администрирование")
	hash, err := shared.HashPassword(password)
	if err != nil {
		return err
	}
	var departmentID int64
	if err := db.QueryRow(ctx, `
		insert into app.departments (name)
		values ($1)
		on conflict (name) do update set name = excluded.name
		returning id
	`, departmentName).Scan(&departmentID); err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		insert into app.users (email, password_hash, full_name, role, department_id, is_active, is_approved)
		values ($1, $2, $3, 'root', $4, true, true)
		on conflict (email) do update
		set password_hash = excluded.password_hash,
			full_name = excluded.full_name,
			role = excluded.role,
			department_id = excluded.department_id,
			is_active = true,
			is_approved = true,
			updated_at = now()
	`, email, hash, fullName, departmentID)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		update app.users
		set is_approved = true,
			is_active = true,
			updated_at = now()
		where role = 'root' and (is_approved = false or is_active = false)
	`)
	return err
}

// validRole централизует проверку для последующего ветвления логики.
func validRole(role string) bool {
	switch strings.TrimSpace(role) {
	case shared.RoleRoot, shared.RoleManager, shared.RoleUser:
		return true
	default:
		return false
	}
}

// getenv изолирует небольшой важный helper для общего сценария.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

var _ = pgx.ErrNoRows
