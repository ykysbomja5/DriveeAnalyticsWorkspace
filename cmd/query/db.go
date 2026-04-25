package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

// verifyReadOnlyConnection выполняет отдельный шаг окружающего сервисного сценария.
func verifyReadOnlyConnection(ctx context.Context, pool *pgxpool.Pool) error {
	var currentUser string
	var canRead bool
	var hasAppSchemaUsage bool
	err := pool.QueryRow(ctx, `
		select
			current_user,
			has_table_privilege(current_user, 'analytics.v_ride_metrics', 'SELECT')
				and has_table_privilege(current_user, 'analytics.v_incity_orders_latest', 'SELECT')
				and has_table_privilege(current_user, 'analytics.v_driver_daily_metrics', 'SELECT')
				and has_table_privilege(current_user, 'analytics.v_passenger_daily_metrics', 'SELECT'),
			has_schema_privilege(current_user, 'app', 'USAGE')
	`).Scan(&currentUser, &canRead, &hasAppSchemaUsage)
	if err != nil {
		return err
	}
	if !canRead {
		return fmt.Errorf("user %q cannot read all required analytics views", currentUser)
	}
	if hasAppSchemaUsage {
		return fmt.Errorf("user %q has access to schema app, expected an isolated read-only user", currentUser)
	}
	return nil
}

// openVerifiedReadOnlyPool создаёт проверенное подключение к внешней зависимости.
func openVerifiedReadOnlyPool(ctx context.Context, auditPool *pgxpool.Pool, execDSN string) (*pgxpool.Pool, error) {
	execPool, err := shared.OpenPostgres(ctx, execDSN)
	if err != nil {
		repaired, repairErr := tryRepairReadOnlyRole(ctx, auditPool, execDSN, err)
		if repairErr != nil {
			log.Printf("read-only role repair attempt failed: %v", repairErr)
		}
		if repaired {
			execPool, err = shared.OpenPostgres(ctx, execDSN)
		}
		if err != nil {
			return nil, err
		}
	}

	if err := verifyReadOnlyConnection(ctx, execPool); err != nil {
		execPool.Close()
		repaired, repairErr := repairReadOnlyRole(ctx, auditPool, execDSN)
		if repairErr != nil {
			return nil, err
		}
		if !repaired {
			return nil, err
		}

		execPool, err = shared.OpenPostgres(ctx, execDSN)
		if err != nil {
			return nil, err
		}
		if err := verifyReadOnlyConnection(ctx, execPool); err != nil {
			execPool.Close()
			return nil, err
		}
	}

	return execPool, nil
}

// tryRepairReadOnlyRole выполняет отдельный шаг окружающего сервисного сценария.
func tryRepairReadOnlyRole(ctx context.Context, auditPool *pgxpool.Pool, execDSN string, connectErr error) (bool, error) {
	if !looksLikeReadOnlyAuthError(connectErr) {
		return false, nil
	}
	return repairReadOnlyRole(ctx, auditPool, execDSN)
}

// repairReadOnlyRole выполняет отдельный шаг окружающего сервисного сценария.
func repairReadOnlyRole(ctx context.Context, auditPool *pgxpool.Pool, execDSN string) (bool, error) {
	cfg, err := pgxpool.ParseConfig(execDSN)
	if err != nil {
		return false, err
	}

	roleName := strings.TrimSpace(cfg.ConnConfig.User)
	rolePassword := cfg.ConnConfig.Password
	databaseName := strings.TrimSpace(cfg.ConnConfig.Database)
	if roleName == "" {
		return false, fmt.Errorf("PG_READONLY_DSN must include a username")
	}
	if rolePassword == "" {
		return false, fmt.Errorf("PG_READONLY_DSN must include a password so the read-only role can be provisioned")
	}

	statements := []string{
		fmt.Sprintf(`
do $$
begin
    if not exists (select 1 from pg_roles where rolname = %s) then
        execute format('create role %%I login password %%L', %s, %s);
    else
        execute format('alter role %%I with login password %%L', %s, %s);
    end if;
end $$;`, quoteSQLLiteral(roleName), quoteSQLLiteral(roleName), quoteSQLLiteral(rolePassword), quoteSQLLiteral(roleName), quoteSQLLiteral(rolePassword)),
	}

	if databaseName != "" {
		statements = append(statements, fmt.Sprintf("grant connect on database %s to %s", quoteSQLIdentifier(databaseName), quoteSQLIdentifier(roleName)))
	}
	statements = append(statements,
		fmt.Sprintf("revoke all privileges on schema app from %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("revoke all privileges on all tables in schema app from %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("revoke all privileges on all sequences in schema app from %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant usage on schema analytics to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.incity to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.driver_detail to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.pass_detail to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.v_incity_orders_latest to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.v_ride_metrics to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.v_driver_daily_metrics to %s", quoteSQLIdentifier(roleName)),
		fmt.Sprintf("grant select on analytics.v_passenger_daily_metrics to %s", quoteSQLIdentifier(roleName)),
	)

	for _, stmt := range statements {
		if _, err := auditPool.Exec(ctx, stmt); err != nil {
			return false, err
		}
	}

	log.Printf("read-only role %q was repaired from PG_DSN and PG_READONLY_DSN settings", roleName)
	return true, nil
}

// looksLikeReadOnlyAuthError централизует проверку для последующего ветвления логики.
func looksLikeReadOnlyAuthError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "28p01") ||
		strings.Contains(text, "password authentication failed") ||
		strings.Contains(text, "failed sasl auth") ||
		strings.Contains(text, "authentication failed")
}

// quoteSQLIdentifier нормализует граничные значения перед дальнейшим использованием.
func quoteSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// quoteSQLLiteral нормализует граничные значения перед дальнейшим использованием.
func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// executeQuery координирует побочные эффекты выполнения и фиксирует результат.
func (app application) executeQuery(ctx context.Context, sqlText string) (shared.QueryResult, error) {
	execTimeout := app.execTimeout
	if execTimeout <= 0 {
		execTimeout = 60 * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	log.Printf("query db execution start: timeout=%s sql=%q", execTimeout, truncateForLog(sqlText, 360))
	rows, err := app.execDB.Query(execCtx, shared.WrapQueryForExecution(sqlText))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return shared.QueryResult{}, &shared.GuardrailError{
				Code:    "timeout",
				Message: "Запрос остановлен guardrails: превышено допустимое время выполнения.",
			}
		}
		return shared.QueryResult{}, err
	}
	defer rows.Close()
	log.Printf("query db rows opened")

	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]string, 0, len(fieldDescriptions))
	for _, field := range fieldDescriptions {
		columns = append(columns, string(field.Name))
	}

	result := shared.QueryResult{Columns: columns, Rows: make([][]string, 0)}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return shared.QueryResult{}, err
		}
		row := make([]string, len(values))
		for i, value := range values {
			row[i] = formatCell(value)
		}
		result.Rows = append(result.Rows, row)
	}
	result.Count = len(result.Rows)
	if err := rows.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(execCtx.Err(), context.DeadlineExceeded) {
			return shared.QueryResult{}, &shared.GuardrailError{
				Code:    "timeout",
				Message: "Запрос остановлен guardrails: превышено допустимое время выполнения.",
			}
		}
		return shared.QueryResult{}, err
	}
	log.Printf("query db rows read: rows=%d", result.Count)
	return result, nil
}
