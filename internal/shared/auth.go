package shared

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	RoleRoot    = "root"
	RoleManager = "manager"
	RoleUser    = "user"

	DefaultPasswordIterations = 210000
	SessionTTL                = 24 * time.Hour
)

// AuthUser объединяет данные, нужные окружающему рабочему процессу.
type AuthUser struct {
	ID             int64  `json:"id"`
	Email          string `json:"email"`
	FullName       string `json:"full_name"`
	Role           string `json:"role"`
	DepartmentID   int64  `json:"department_id,omitempty"`
	DepartmentName string `json:"department_name,omitempty"`
	IsActive       bool   `json:"is_active"`
	IsApproved     bool   `json:"is_approved"`
}

// PasswordSalt выполняет отдельный шаг окружающего сервисного сценария.
func PasswordSalt() string {
	value := strings.TrimSpace(os.Getenv("PASSWORD_SALT"))
	if value == "" {
		// Дефолт допустим только для локальной разработки; в реальном окружении соль должна быть задана явно.
		return "drivee-local-dev-change-me"
	}
	return value
}

// HashPassword централизует проверку для последующего ветвления логики.
func HashPassword(password string) (string, error) {
	if strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("password is required")
	}
	// Пароль никогда не хранится в открытом виде: в БД уходит только PBKDF2-хеш.
	derived, err := pbkdf2.Key(sha256.New, password, []byte(PasswordSalt()), DefaultPasswordIterations, 32)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbkdf2-sha256$%d$%s", DefaultPasswordIterations, base64.RawStdEncoding.EncodeToString(derived)), nil
}

// VerifyPassword выполняет отдельный шаг окружающего сервисного сценария.
func VerifyPassword(password, stored string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 3 || parts[0] != "pbkdf2-sha256" {
		return false
	}
	expected, err := HashPassword(password)
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(expected), []byte(stored))
}

// NewToken выполняет отдельный шаг окружающего сервисного сценария.
func NewToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	// В storage сохраняется только хеш токена, чтобы активные сессии нельзя было восстановить из БД.
	token := base64.RawURLEncoding.EncodeToString(raw)
	return token, TokenHash(token), nil
}

// TokenHash выполняет отдельный шаг окружающего сервисного сценария.
func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}

// BearerToken выполняет отдельный шаг окружающего сервисного сценария.
func BearerToken(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return ""
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

// UserFromBearer выполняет отдельный шаг окружающего сервисного сценария.
func UserFromBearer(ctx context.Context, db *pgxpool.Pool, token string) (AuthUser, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return AuthUser{}, fmt.Errorf("authorization token is required")
	}
	row := db.QueryRow(ctx, `
		select
			u.id,
			u.email,
			u.full_name,
			u.role,
			coalesce(u.department_id, 0),
			coalesce(d.name, ''),
			u.is_approved
		from app.user_sessions s
		join app.users u on u.id = s.user_id
		left join app.departments d on d.id = u.department_id
		where s.token_hash = $1
			and s.expires_at > now()
			and u.is_active = true
	`, TokenHash(token))
	var user AuthUser
	if err := row.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName, &user.IsApproved); err != nil {
		if err == pgx.ErrNoRows {
			return AuthUser{}, fmt.Errorf("invalid or expired token")
		}
		return AuthUser{}, err
	}
	return user, nil
}

// RequireUser выполняет отдельный шаг окружающего сервисного сценария.
func RequireUser(ctx context.Context, db *pgxpool.Pool, r *http.Request) (AuthUser, error) {
	return UserFromBearer(ctx, db, BearerToken(r))
}

// EnsureIdentitySchema подготавливает или проверяет нужное состояние БД и приложения.
func EnsureIdentitySchema(ctx context.Context, db *pgxpool.Pool) error {
	statements := []string{
		`create schema if not exists app`,
		`create table if not exists app.departments (
			id bigserial primary key,
			name text not null unique,
			created_at timestamptz not null default now()
		)`,
		`create table if not exists app.users (
			id bigserial primary key,
			email text not null unique,
			password_hash text not null,
			full_name text not null,
			role text not null default 'user' check (role in ('root', 'manager', 'user')),
			department_id bigint references app.departments(id) on delete set null,
			is_active boolean not null default true,
			is_approved boolean not null default false,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		)`,
		`alter table app.users add column if not exists is_approved boolean not null default false`,
		`create table if not exists app.user_sessions (
			id bigserial primary key,
			user_id bigint not null references app.users(id) on delete cascade,
			token_hash text not null unique,
			expires_at timestamptz not null,
			created_at timestamptz not null default now()
		)`,
		`create table if not exists app.department_access (
			id bigserial primary key,
			user_id bigint not null references app.users(id) on delete cascade,
			department_id bigint not null references app.departments(id) on delete cascade,
			granted_by bigint references app.users(id) on delete set null,
			created_at timestamptz not null default now(),
			unique (user_id, department_id)
		)`,
		`alter table app.saved_reports add column if not exists owner_user_id bigint references app.users(id) on delete set null`,
		`alter table app.report_templates add column if not exists owner_user_id bigint references app.users(id) on delete set null`,
		`create index if not exists idx_users_department on app.users (department_id, role)`,
		`create index if not exists idx_user_sessions_user on app.user_sessions (user_id, expires_at desc)`,
		`create index if not exists idx_department_access_user on app.department_access (user_id, department_id)`,
		`create index if not exists idx_saved_reports_owner_user on app.saved_reports (owner_user_id, updated_at desc)`,
		`create index if not exists idx_report_templates_owner_user on app.report_templates (owner_user_id, updated_at desc)`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
