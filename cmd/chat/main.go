package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"drivee-self-service/internal/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// application объединяет зависимости сервиса для HTTP-обработчиков.
type application struct {
	db  *pgxpool.Pool
	hub *roomHub
}

// roomHub объединяет данные, нужные окружающему рабочему процессу.
type roomHub struct {
	mu      sync.RWMutex
	clients map[int64]map[chan chatMessage]struct{}
}

// chatRoom объединяет данные, нужные окружающему рабочему процессу.
type chatRoom struct {
	ID        int64             `json:"id"`
	Title     string            `json:"title"`
	CreatedBy shared.AuthUser   `json:"created_by"`
	Members   []shared.AuthUser `json:"members,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// chatMessage объединяет данные, нужные окружающему рабочему процессу.
type chatMessage struct {
	ID              int64           `json:"id"`
	RoomID          int64           `json:"room_id"`
	Sender          shared.AuthUser `json:"sender"`
	Body            string          `json:"body,omitempty"`
	AttachmentType  string          `json:"attachment_type,omitempty"`
	AttachmentID    int64           `json:"attachment_id,omitempty"`
	AttachmentTitle string          `json:"attachment_title,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

// createRoomRequest описывает JSON-запрос на границе API.
type createRoomRequest struct {
	Title     string  `json:"title"`
	MemberIDs []int64 `json:"member_ids"`
}

// addMembersRequest описывает JSON-запрос на границе API.
type addMembersRequest struct {
	MemberIDs []int64 `json:"member_ids"`
}

// sendMessageRequest описывает JSON-запрос на границе API.
type sendMessageRequest struct {
	Body           string `json:"body"`
	AttachmentType string `json:"attachment_type,omitempty"`
	AttachmentID   int64  `json:"attachment_id,omitempty"`
}

// main связывает конфигурацию, хранилище, маршруты и запускает сервис.
func main() {
	if err := shared.LoadDotEnv(".env"); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}
	dsn := strings.TrimSpace(os.Getenv("PG_DSN"))
	if dsn == "" {
		log.Fatal("PG_DSN is required for chat service")
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
	if err := ensureChatSchema(ctx, pool); err != nil {
		log.Fatalf("failed to prepare chat schema: %v", err)
	}

	app := application{db: pool, hub: &roomHub{clients: map[int64]map[chan chatMessage]struct{}{}}}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/chats/assets", app.handleAssets)
	mux.HandleFunc("/api/v1/chats", app.handleRooms)
	mux.HandleFunc("/api/v1/chats/", app.handleRoomActions)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "chat"})
	})

	port := getenv("PORT", getenv("CHAT_PORT", "8086"))
	log.Printf("chat listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// ensureChatSchema подготавливает или проверяет нужное состояние БД и приложения.
func ensureChatSchema(ctx context.Context, db *pgxpool.Pool) error {
	statements := []string{
		`create table if not exists app.chat_rooms (
			id bigserial primary key,
			title text not null,
			created_by bigint not null references app.users(id) on delete cascade,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		)`,
		`create table if not exists app.chat_room_members (
			room_id bigint not null references app.chat_rooms(id) on delete cascade,
			user_id bigint not null references app.users(id) on delete cascade,
			added_by bigint references app.users(id) on delete set null,
			created_at timestamptz not null default now(),
			primary key (room_id, user_id)
		)`,
		`create table if not exists app.chat_messages (
			id bigserial primary key,
			room_id bigint not null references app.chat_rooms(id) on delete cascade,
			sender_id bigint not null references app.users(id) on delete cascade,
			body text not null default '',
			attachment_type text check (attachment_type is null or attachment_type in ('report', 'template')),
			attachment_id bigint,
			attachment_title text not null default '',
			created_at timestamptz not null default now()
		)`,
		`create index if not exists idx_chat_members_user on app.chat_room_members (user_id, room_id)`,
		`create index if not exists idx_chat_messages_room on app.chat_messages (room_id, id desc)`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// handleRooms проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleRooms(w http.ResponseWriter, r *http.Request) {
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
		rooms, err := app.listRooms(r.Context(), user.ID)
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusOK, rooms)
	case http.MethodPost:
		var req createRoomRequest
		if err := shared.DecodeJSON(r, &req); err != nil {
			shared.WriteError(w, http.StatusBadRequest, "invalid json")
			return
		}
		room, err := app.createRoom(r.Context(), user, req)
		if err != nil {
			shared.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		shared.WriteJSON(w, http.StatusCreated, room)
	default:
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleRoomActions проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleRoomActions(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	user, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/chats/"), "/"), "/")
	if len(parts) < 2 {
		shared.WriteError(w, http.StatusNotFound, "chat route not found")
		return
	}
	roomID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid room id")
		return
	}
	if ok, err := app.isMember(r.Context(), roomID, user.ID); err != nil || !ok {
		shared.WriteError(w, http.StatusForbidden, "chat room is not available")
		return
	}
	switch {
	case len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodGet:
		app.handleMessages(w, r, roomID)
	case len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodPost:
		app.handleSendMessage(w, r, roomID, user)
	case len(parts) == 2 && parts[1] == "members" && r.Method == http.MethodPost:
		app.handleAddMembers(w, r, roomID, user)
	case len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet:
		app.handleEvents(w, r, roomID)
	default:
		shared.WriteError(w, http.StatusNotFound, "chat route not found")
	}
}

// handleMessages проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleMessages(w http.ResponseWriter, r *http.Request, roomID int64) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	messages, err := app.listMessages(r.Context(), roomID, after)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, messages)
}

// handleSendMessage проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleSendMessage(w http.ResponseWriter, r *http.Request, roomID int64, user shared.AuthUser) {
	var req sendMessageRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	message, err := app.saveMessage(r.Context(), roomID, user, req)
	if err != nil {
		shared.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	app.hub.publish(roomID, message)
	shared.WriteJSON(w, http.StatusCreated, message)
}

// handleAddMembers проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleAddMembers(w http.ResponseWriter, r *http.Request, roomID int64, user shared.AuthUser) {
	var req addMembersRequest
	if err := shared.DecodeJSON(r, &req); err != nil {
		shared.WriteError(w, http.StatusBadRequest, "invalid json")
		return
	}
	for _, memberID := range req.MemberIDs {
		if memberID <= 0 {
			continue
		}
		_, err := app.db.Exec(r.Context(), `
			insert into app.chat_room_members (room_id, user_id, added_by)
			values ($1, $2, $3)
			on conflict (room_id, user_id) do nothing
		`, roomID, memberID, user.ID)
		if err != nil {
			shared.WriteError(w, http.StatusBadGateway, err.Error())
			return
		}
	}
	room, err := app.fetchRoom(r.Context(), roomID)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, room)
}

// handleEvents проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleEvents(w http.ResponseWriter, r *http.Request, roomID int64) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher, ok := w.(http.Flusher)
	if !ok {
		shared.WriteError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	ch := app.hub.subscribe(roomID)
	defer app.hub.unsubscribe(roomID, ch)
	fmt.Fprintf(w, "event: ready\ndata: {}\n\n")
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case message := <-ch:
			// SSE нужен, чтобы новые сообщения появлялись в чате без ручного обновления страницы.
			raw, _ := json.Marshal(message)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", raw)
			flusher.Flush()
		case <-time.After(25 * time.Second):
			fmt.Fprintf(w, "event: ping\ndata: {}\n\n")
			flusher.Flush()
		}
	}
}

// handleAssets проверяет HTTP-запрос и запускает сценарий эндпоинта.
func (app application) handleAssets(w http.ResponseWriter, r *http.Request) {
	if shared.HandlePreflight(w, r) {
		return
	}
	user, err := shared.RequireUser(r.Context(), app.db, r)
	if err != nil {
		shared.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if r.Method != http.MethodGet {
		shared.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	reports, templates, err := app.listAssets(r.Context(), user)
	if err != nil {
		shared.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	shared.WriteJSON(w, http.StatusOK, map[string]any{"reports": reports, "templates": templates})
}

// createRoom выполняет изменение с учётом владения и правил валидации.
func (app application) createRoom(ctx context.Context, user shared.AuthUser, req createRoomRequest) (chatRoom, error) {
	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = "Новый чат"
	}
	var roomID int64
	if err := app.db.QueryRow(ctx, `
		insert into app.chat_rooms (title, created_by)
		values ($1, $2)
		returning id
	`, title, user.ID).Scan(&roomID); err != nil {
		return chatRoom{}, err
	}
	memberIDs := append([]int64{user.ID}, req.MemberIDs...)
	for _, memberID := range uniqueInt64(memberIDs) {
		if memberID <= 0 {
			continue
		}
		_, err := app.db.Exec(ctx, `
			insert into app.chat_room_members (room_id, user_id, added_by)
			values ($1, $2, $3)
			on conflict (room_id, user_id) do nothing
		`, roomID, memberID, user.ID)
		if err != nil {
			return chatRoom{}, err
		}
	}
	return app.fetchRoom(ctx, roomID)
}

// saveMessage выполняет изменение с учётом владения и правил валидации.
func (app application) saveMessage(ctx context.Context, roomID int64, user shared.AuthUser, req sendMessageRequest) (chatMessage, error) {
	body := strings.TrimSpace(req.Body)
	attachmentType := strings.TrimSpace(req.AttachmentType)
	if body == "" && attachmentType == "" {
		return chatMessage{}, fmt.Errorf("message body or attachment is required")
	}
	if attachmentType != "" && attachmentType != "report" && attachmentType != "template" {
		return chatMessage{}, fmt.Errorf("attachment_type must be report or template")
	}
	attachmentTitle := ""
	if attachmentType != "" {
		title, err := app.ownedAssetTitle(ctx, user, attachmentType, req.AttachmentID)
		if err != nil {
			return chatMessage{}, err
		}
		attachmentTitle = title
	}

	tx, err := app.db.Begin(ctx)
	if err != nil {
		return chatMessage{}, err
	}
	defer tx.Rollback(ctx)

	if attachmentType == "report" {
		if err := publishReportAttachmentTx(ctx, tx, user, req.AttachmentID); err != nil {
			return chatMessage{}, err
		}
	}
	if attachmentType == "template" {
		if err := publishTemplateAttachmentTx(ctx, tx, user, req.AttachmentID); err != nil {
			return chatMessage{}, err
		}
	}

	row := tx.QueryRow(ctx, `
		insert into app.chat_messages (room_id, sender_id, body, attachment_type, attachment_id, attachment_title)
		values ($1, $2, $3, nullif($4, ''), nullif($5, 0), $6)
		returning id, created_at
	`, roomID, user.ID, body, attachmentType, req.AttachmentID, attachmentTitle)
	message := chatMessage{
		RoomID:          roomID,
		Sender:          user,
		Body:            body,
		AttachmentType:  attachmentType,
		AttachmentID:    req.AttachmentID,
		AttachmentTitle: attachmentTitle,
	}
	if err := row.Scan(&message.ID, &message.CreatedAt); err != nil {
		return chatMessage{}, err
	}
	if _, err := tx.Exec(ctx, `update app.chat_rooms set updated_at = now() where id = $1`, roomID); err != nil {
		return chatMessage{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return chatMessage{}, err
	}
	return message, nil
}

// listRooms загружает записи, доступные текущему пользователю или сценарию.
func (app application) listRooms(ctx context.Context, userID int64) ([]chatRoom, error) {
	rows, err := app.db.Query(ctx, `
		select cr.id, cr.title, cr.updated_at, u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, '')
		from app.chat_rooms cr
		join app.chat_room_members crm on crm.room_id = cr.id
		join app.users u on u.id = cr.created_by
		left join app.departments d on d.id = u.department_id
		where crm.user_id = $1
		order by cr.updated_at desc, cr.id desc
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rooms := []chatRoom{}
	for rows.Next() {
		var room chatRoom
		if err := rows.Scan(&room.ID, &room.Title, &room.UpdatedAt, &room.CreatedBy.ID, &room.CreatedBy.Email, &room.CreatedBy.FullName, &room.CreatedBy.Role, &room.CreatedBy.DepartmentID, &room.CreatedBy.DepartmentName); err != nil {
			return nil, err
		}
		room.Members, _ = app.roomMembers(ctx, room.ID)
		rooms = append(rooms, room)
	}
	return rooms, rows.Err()
}

// fetchRoom загружает записи, доступные текущему пользователю или сценарию.
func (app application) fetchRoom(ctx context.Context, roomID int64) (chatRoom, error) {
	row := app.db.QueryRow(ctx, `
		select cr.id, cr.title, cr.updated_at, u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, '')
		from app.chat_rooms cr
		join app.users u on u.id = cr.created_by
		left join app.departments d on d.id = u.department_id
		where cr.id = $1
	`, roomID)
	var room chatRoom
	if err := row.Scan(&room.ID, &room.Title, &room.UpdatedAt, &room.CreatedBy.ID, &room.CreatedBy.Email, &room.CreatedBy.FullName, &room.CreatedBy.Role, &room.CreatedBy.DepartmentID, &room.CreatedBy.DepartmentName); err != nil {
		return chatRoom{}, err
	}
	room.Members, _ = app.roomMembers(ctx, room.ID)
	return room, nil
}

// roomMembers выполняет отдельный шаг окружающего сервисного сценария.
func (app application) roomMembers(ctx context.Context, roomID int64) ([]shared.AuthUser, error) {
	rows, err := app.db.Query(ctx, `
		select u.id, u.email, u.full_name, u.role, coalesce(u.department_id, 0), coalesce(d.name, '')
		from app.chat_room_members crm
		join app.users u on u.id = crm.user_id
		left join app.departments d on d.id = u.department_id
		where crm.room_id = $1
		order by u.full_name
	`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := []shared.AuthUser{}
	for rows.Next() {
		var user shared.AuthUser
		if err := rows.Scan(&user.ID, &user.Email, &user.FullName, &user.Role, &user.DepartmentID, &user.DepartmentName); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, rows.Err()
}

// listMessages загружает записи, доступные текущему пользователю или сценарию.
func (app application) listMessages(ctx context.Context, roomID int64, afterID int64) ([]chatMessage, error) {
	rows, err := app.db.Query(ctx, `
		select
			cm.id,
			cm.room_id,
			cm.body,
			coalesce(cm.attachment_type, ''),
			coalesce(cm.attachment_id, 0),
			cm.attachment_title,
			cm.created_at,
			u.id,
			u.email,
			u.full_name,
			u.role,
			coalesce(u.department_id, 0),
			coalesce(d.name, '')
		from app.chat_messages cm
		join app.users u on u.id = cm.sender_id
		left join app.departments d on d.id = u.department_id
		where cm.room_id = $1 and cm.id > $2
		order by cm.id asc
		limit 200
	`, roomID, afterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := []chatMessage{}
	for rows.Next() {
		var message chatMessage
		if err := rows.Scan(&message.ID, &message.RoomID, &message.Body, &message.AttachmentType, &message.AttachmentID, &message.AttachmentTitle, &message.CreatedAt, &message.Sender.ID, &message.Sender.Email, &message.Sender.FullName, &message.Sender.Role, &message.Sender.DepartmentID, &message.Sender.DepartmentName); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

// isMember централизует проверку для последующего ветвления логики.
func (app application) isMember(ctx context.Context, roomID, userID int64) (bool, error) {
	var exists bool
	err := app.db.QueryRow(ctx, `select exists(select 1 from app.chat_room_members where room_id = $1 and user_id = $2)`, roomID, userID).Scan(&exists)
	return exists, err
}

// listAssets загружает записи, доступные текущему пользователю или сценарию.
func (app application) listAssets(ctx context.Context, user shared.AuthUser) ([]map[string]any, []map[string]any, error) {
	reports, err := app.listReportAssets(ctx, user)
	if err != nil {
		return nil, nil, err
	}
	templates, err := app.listTemplateAssets(ctx, user)
	if err != nil {
		return nil, nil, err
	}
	return reports, templates, nil
}

// listReportAssets загружает записи, доступные текущему пользователю или сценарию.
func (app application) listReportAssets(ctx context.Context, user shared.AuthUser) ([]map[string]any, error) {
	rows, err := app.db.Query(ctx, `
		select id, name, updated_at
		from app.saved_reports
		where owner_user_id = $1 or ($1 = 0 and owner_name = $2) or owner_name = $2
		order by updated_at desc
		limit 100
	`, user.ID, user.FullName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id int64
		var name string
		var updatedAt time.Time
		if err := rows.Scan(&id, &name, &updatedAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"id": id, "name": name, "updated_at": updatedAt})
	}
	return items, rows.Err()
}

// listTemplateAssets загружает записи, доступные текущему пользователю или сценарию.
func (app application) listTemplateAssets(ctx context.Context, user shared.AuthUser) ([]map[string]any, error) {
	rows, err := app.db.Query(ctx, `
		select id, name, updated_at
		from app.report_templates
		where owner_user_id = $1 or ($1 = 0 and owner_name = $2) or owner_name = $2
		order by updated_at desc
		limit 100
	`, user.ID, user.FullName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id int64
		var name string
		var updatedAt time.Time
		if err := rows.Scan(&id, &name, &updatedAt); err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"id": id, "name": name, "updated_at": updatedAt})
	}
	return items, rows.Err()
}

// ownedAssetTitle выполняет отдельный шаг окружающего сервисного сценария.
func (app application) ownedAssetTitle(ctx context.Context, user shared.AuthUser, attachmentType string, attachmentID int64) (string, error) {
	if attachmentID == 0 {
		return "", fmt.Errorf("attachment_id is required")
	}
	table := "app.saved_reports"
	if attachmentType == "template" {
		table = "app.report_templates"
	}
	query := fmt.Sprintf(`select name from %s where id = $1 and (owner_user_id = $2 or owner_name = $3)`, table)
	var title string
	if err := app.db.QueryRow(ctx, query, attachmentID, user.ID, user.FullName).Scan(&title); err != nil {
		if err == pgx.ErrNoRows {
			return "", fmt.Errorf("attachment is not available")
		}
		return "", err
	}
	return title, nil
}

// publishReportAttachmentTx выполняет отдельный шаг окружающего сервисного сценария.
func publishReportAttachmentTx(ctx context.Context, tx pgx.Tx, user shared.AuthUser, reportID int64) error {
	tag, err := tx.Exec(ctx, `
		update app.saved_reports
		set
			is_public = true,
			owner_department = coalesce(nullif(owner_department, ''), $4),
			updated_at = now()
		where id = $1
			and (owner_user_id = $2 or owner_name = $3)
	`, reportID, user.ID, user.FullName, user.DepartmentName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("attachment is not available")
	}
	return nil
}

// publishTemplateAttachmentTx выполняет отдельный шаг окружающего сервисного сценария.
func publishTemplateAttachmentTx(ctx context.Context, tx pgx.Tx, user shared.AuthUser, templateID int64) error {
	tag, err := tx.Exec(ctx, `
		update app.report_templates
		set
			is_public = true,
			owner_department = coalesce(nullif(owner_department, ''), $4),
			updated_at = now()
		where id = $1
			and (owner_user_id = $2 or owner_name = $3)
	`, templateID, user.ID, user.FullName, user.DepartmentName)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("attachment is not available")
	}
	return nil
}

// subscribe выполняет отдельный шаг окружающего сервисного сценария.
func (h *roomHub) subscribe(roomID int64) chan chatMessage {
	ch := make(chan chatMessage, 16)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[roomID] == nil {
		h.clients[roomID] = map[chan chatMessage]struct{}{}
	}
	h.clients[roomID][ch] = struct{}{}
	return ch
}

// unsubscribe выполняет отдельный шаг окружающего сервисного сценария.
func (h *roomHub) unsubscribe(roomID int64, ch chan chatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[roomID] != nil {
		delete(h.clients[roomID], ch)
	}
	close(ch)
}

// publish выполняет отдельный шаг окружающего сервисного сценария.
func (h *roomHub) publish(roomID int64, message chatMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients[roomID] {
		select {
		case ch <- message:
		default:
		}
	}
}

// uniqueInt64 выполняет отдельный шаг окружающего сервисного сценария.
func uniqueInt64(values []int64) []int64 {
	seen := map[int64]struct{}{}
	result := []int64{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// getenv изолирует небольшой важный helper для общего сценария.
func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
