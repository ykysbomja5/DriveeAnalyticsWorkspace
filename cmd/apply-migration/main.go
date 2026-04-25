package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:qwe123asdzxc@localhost:5432/drivee_analytics?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Проверяем подключение.
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	fmt.Println("Connected to database. Applying migration...")

	// Добавляем колонку is_approved.
	_, err = pool.Exec(ctx, `
		alter table app.users 
		add column if not exists is_approved boolean not null default false
	`)
	if err != nil {
		log.Fatalf("Failed to add is_approved column: %v", err)
	}
	fmt.Println("✓ Added is_approved column")

	// Помечаем существующих пользователей как подтверждённых.
	result, err := pool.Exec(ctx, `
		update app.users 
		set is_approved = true 
		where is_approved = false
	`)
	if err != nil {
		log.Fatalf("Failed to update existing users: %v", err)
	}
	fmt.Printf("✓ Updated %d existing users to approved status\n", result.RowsAffected())

	fmt.Println("\nMigration completed successfully!")
}
