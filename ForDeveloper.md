# ForDeveloper

## Назначение документа

Этот файл нужен разработчикам, которые будут сопровождать и расширять проект. Ниже подробно описаны архитектура, стек, потоки данных, назначение каталогов и ключевых файлов, а также точки расширения системы.

## 1. Общая идея проекта

Проект реализует внутреннюю self-service аналитическую платформу для бизнес-пользователей. Пользователь пишет вопрос на русском языке, а система:

1. получает текст вопроса;
2. отправляет текст в Qwen через Cerebras Cloud;
3. получает JSON с SQL, `intent`, уточнениями и confidence;
4. валидирует SQL через backend guardrails;
5. выполняет запрос к PostgreSQL под read-only пользователем;
6. возвращает таблицу, визуализацию и explainability;
7. при необходимости сохраняет отчёт, шаблон или публикует их в чат.

Ключевая архитектурная идея: Qwen генерирует SQL, но не получает прямой доступ к БД. Финальное право на выполнение остаётся у backend guardrails и read-only подключения.

## 2. Используемые технологии

### Backend

- `Go 1.26`
- `net/http` для HTTP API
- `github.com/jackc/pgx/v5` для работы с PostgreSQL
- `pgxpool` для connection pooling
- `gofpdf` для PDF-экспорта отчётов

### Database

- `PostgreSQL 16`
- Схемы:
  - `analytics` — аналитические данные и представления
  - `app` — служебные таблицы приложения

### Frontend

- `HTML`
- `CSS`
- `Vanilla JavaScript`
- Без React/Vue/Angular
- Frontend раздаётся напрямую через `gateway`

### Infrastructure / local dev

- `PowerShell` для локального orchestration
- `docker-compose.yml` для быстрого поднятия PostgreSQL

### LLM layer

- `Qwen 3 235B Instruct` через Cerebras Cloud
- отдельный файл настроек `config/qwen_sql_settings.md`
- Qwen и локальный Qwen parser удалены из цепочки
- модель возвращает SQL, который обязательно проверяется guardrails

## 3. Архитектурная схема

```text
Browser
  -> gateway
     -> auth
     -> query
        -> meta
        -> llm
        -> postgres (readonly for query execution)
     -> reports
     -> chat

postgres
  -> analytics schema
  -> app schema
```

## 4. Роли сервисов

### `cmd/gateway`

Назначение:

- единая точка входа для браузера;
- раздача статических файлов из `web/`;
- reverse proxy во внутренние сервисы;
- проверка авторизации перед доступом к рабочим API;
- проброс user-context в заголовках:
  - `X-Drivee-User`
  - `X-Drivee-Department`
  - `X-Drivee-Role`
  - `X-Drivee-User-Id`

Почему это важно:

- frontend не должен знать адреса всех внутренних сервисов;
- безопасность и аутентификация централизуются в одном месте;
- downstream-сервисы получают уже проверенный контекст пользователя.

Ключевой файл:

- `cmd/gateway/main.go`

### `cmd/auth`

Назначение:

- регистрация и логин;
- выдача и отзыв сессий;
- bootstrap root-пользователя;
- список пользователей;
- подтверждение пользователей;
- роли `root`, `manager`, `user`;
- выдача доступа к отделам.

Особенности:

- использует `PG_DSN`;
- поднимает identity-объекты через shared-логику;
- root-пользователь автоматически создаётся на старте из `.env`.

Ключевой файл:

- `cmd/auth/main.go`

### `cmd/meta`

Назначение:

- отдаёт semantic layer;
- публикует список метрик, измерений, терминов, примеров запросов;
- при наличии БД подтягивает термины из `app.semantic_terms`.

Зачем нужен отдельный сервис:

- semantic layer не жёстко зашит только в UI;
- `query` и `llm` получают общий источник бизнес-описаний;
- можно отдельно развивать словарь и бизнес-термины.

Ключевой файл:

- `cmd/meta/main.go`

### `cmd/llm`

Назначение:

- интерпретация русского текста через Qwen;
- генерация JSON с SQL, intent, confidence и clarification;
- загрузка внешнего файла настроек при каждом запросе;
- интеграция с Cerebras Chat Completions.

Важно:

- сервис не подключается к БД;
- SQL от модели не является доверенным, его проверяет `query` через guardrails;
- файл `config/qwen_sql_settings.md` должен обновляться вместе со схемой БД.

Ключевые файлы:

- `cmd/llm/main.go` — HTTP entrypoint
- `cmd/llm/provider.go` — проверка Qwen/Cerebras конфигурации
- `cmd/llm/qwen.go` — интеграция с Qwen
- `config/qwen_sql_settings.md` — инструкции модели, описание БД и правила SQL

### `cmd/query`

Назначение:

- получает текст запроса;
- запрашивает semantic layer;
- обращается к `llm`;
- при необходимости корректирует относительный период по фактическим данным;
- локально собирает SQL;
- валидирует SQL;
- выполняет его в PostgreSQL под read-only пользователем;
- возвращает preview, intent, SQL, result и chart spec;
- пишет логи в `app.query_logs`.

Это главный orchestration-сервис аналитики.

Ключевые файлы:

- `cmd/query/main.go` — HTTP-слой и orchestration
- `cmd/query/db.go` — работа с БД
- `cmd/query/plan.go` — генерация плана и compose ответа
- `cmd/query/charts.go` — подбор визуализации
- `cmd/query/helpers.go` — служебные функции

### `cmd/reports`

Назначение:

- сохранение отчётов;
- листинг отчётов;
- работа с шаблонами;
- расписание повторного запуска;
- экспорт;
- публикация доступов и владельцев;
- косвенная связь с чатами через вложения.

Особенности:

- на старте проверяет и дозаводит служебные таблицы и индексы;
- запускает scheduler в фоне;
- хранит не только SQL, но и preview/result/provider metadata.

Ключевые файлы:

- `cmd/reports/main.go` — инициализация сервиса
- `cmd/reports/report_handlers.go` — API отчётов
- `cmd/reports/report_store.go` — сохранение и чтение отчётов
- `cmd/reports/template_handlers.go` — API шаблонов
- `cmd/reports/template_store.go` — хранилище шаблонов
- `cmd/reports/template_utils.go` — служебная логика шаблонов
- `cmd/reports/scheduler.go` — расписание автоматических запусков
- `cmd/reports/export.go` — экспорт, включая PDF

### `cmd/chat`

Назначение:

- комнаты;
- участники;
- сообщения;
- публикация отчётов и шаблонов как вложений;
- SSE-поток новых сообщений.

Особенности:

- хранит всё в PostgreSQL;
- использует `roomHub` для live-доставки в рамках процесса;
- проверяет membership комнаты перед чтением/записью.

Ключевой файл:

- `cmd/chat/main.go`

### `cmd/apply-migration`

Назначение:

- утилита для ручного применения SQL-миграций.

## 5. Shared-слой

Каталог `internal/shared/` содержит общий код, который переиспользуется сервисами.

### `internal/shared/contracts.go`

Это центральный файл контрактов проекта. В нём описаны:

- входные и выходные DTO для query API;
- структура `Intent`;
- semantic layer;
- preview/result/chart structs;
- модели отчётов и шаблонов;
- нормализация текста и построение preview.

Если меняется API между сервисами, очень вероятно, что правка начинается именно здесь.

### `internal/shared/llm.go`

Это один из самых важных файлов проекта.

Он отвечает за:

- контракты LLM-запросов и ответов;
- guardrails для SQL, который вернул Qwen;
- allowlist разрешённых таблиц и колонок;
- запрет DDL/DML, служебных схем, комментариев, `SELECT *` и опасных функций;
- безопасную обёртку SQL перед выполнением с лимитом строк.

Именно этот файл решает, можно ли выполнить SQL, который пришёл от модели.

Если нужно:

- добавить новую таблицу/витрину;
- добавить новую колонку;
- разрешить новый тип безопасного аналитического запроса;

то почти всегда придётся править `config/qwen_sql_settings.md`, `contracts.go` и allowlist в `internal/shared/llm.go` вместе.

### `internal/shared/auth.go`

Назначение:

- парольная логика;
- токены;
- извлечение bearer token;
- проверка сессий пользователя.

### `internal/shared/pg.go`

Назначение:

- открытие PostgreSQL connection pool;
- общие DB helper-функции.

### `internal/shared/http.go`

Назначение:

- единый JSON output;
- единая обработка ошибок;
- decode JSON;
- CORS/preflight helpers.

### `internal/shared/env.go`

Назначение:

- загрузка `.env`.

### `internal/shared/contracts_test.go`, `llm_test.go`, `guardrails_test.go`

Покрывают наиболее важные shared-механизмы:

- корректность контрактов;
- поведение LLM helper-логики;
- безопасность и ожидаемое поведение SQL guardrails.

## 6. База данных

### Основной файл схемы

- `db/schema.sql`

Это главный источник правды по схеме базы.

### Схема `analytics`

Используется для аналитических данных.

#### `analytics.incity`

Сырой источник заказов и тендеров.

Типы данных включают:

- идентификаторы города, заказа, тендера, клиента, водителя;
- временные поля жизненного цикла заказа;
- статусы;
- дистанцию;
- длительность;
- цены.

#### `analytics.v_incity_orders_latest`

View, которая:

- дедуплицирует события по `order_id`;
- выбирает актуальное состояние заказа;
- вычисляет флаги завершения и отмены;
- подготавливает order-level слой.

#### `analytics.v_ride_metrics`

Агрегированное представление, по которому работает аналитика MVP.

Содержит:

- `stat_date`
- `city`
- `status_order`
- `status_tender`
- `completed_orders`
- `cancelled_orders`
- `total_orders`
- `gross_revenue_local`
- `avg_price_local`
- `avg_distance_meters`
- `avg_duration_seconds`

Это основной источник для Qwen-настроек, query-сервиса и guardrails.

### Схема `app`

Используется для приложения.

#### Identity / access

- `app.departments`
- `app.users`
- `app.user_sessions`
- `app.department_access`

#### Reports

- `app.saved_reports`
- `app.report_templates`
- `app.report_runs`

#### Query audit

- `app.query_logs`

#### Semantic layer

- `app.semantic_terms`

#### Chat

- `app.chat_rooms`
- `app.chat_room_members`
- `app.chat_messages`

## 7. Frontend

Весь UI лежит в каталоге `web/`.

### Основные HTML-страницы

- `web/index.html` — главная аналитическая рабочая панель
- `web/login.html` — страница входа
- `web/pending-approval.html` — экран ожидания подтверждения пользователя
- `web/profile.html` — профиль пользователя
- `web/glossary.html` — словарь и semantic layer
- `web/templates.html` — шаблоны отчётов
- `web/reports.html` — архив отчётов
- `web/admin.html` — администрирование
- `web/chat.html` — чат

### Основные JS-файлы

- `web/app-core.js`
  - базовое состояние страницы
  - общая загрузка и orchestration главного экрана

- `web/app-intent.js`
  - отрисовка intent/explainability

- `web/app-visuals.js`
  - визуализация результатов

- `web/app-panels.js`
  - работа с панелями и UI-композициями

- `web/app-actions.js`
  - действия пользователя: parse/run/save/export/template

- `web/app.js`
  - legacy entrypoint, сохранён как reference

- `web/auth-guard.js`
  - защита страниц и работа с авторизацией

- `web/login.js`
  - логин/регистрация

- `web/admin.js`
  - пользователи, pending approval, department access

- `web/reports.js`
  - архив отчётов

- `web/templates.js`
  - экран шаблонов

- `web/chat.js`
  - комнаты, сообщения, события

- `web/glossary.js`
  - словарь и semantic layer

- `web/profile.js`
  - профиль пользователя

- `web/pending-approval.js`
  - состояние неподтверждённого пользователя

### CSS

- `web/styles.css`

Содержит общую визуальную систему интерфейса.

## 8. Локальный запуск

Главный файл для запуска:

- `scripts/run-local.ps1`

Что он делает:

- читает `.env`;
- выставляет дефолты;
- проверяет конфликты по портам;
- может остановить старые процессы проекта;
- собирает `.exe`-файлы в `.bin`;
- поднимает сервисы в отдельных окнах;
- автоматически прокидывает внутренние URL между сервисами.

Это рекомендуемый способ локального запуска на Windows.

## 9. Конфигурация и `.env`

Основные переменные:

- `PG_DSN`
- `PG_READONLY_DSN`
- `QUERY_PORT`
- `LLM_PORT`
- `REPORTS_PORT`
- `META_PORT`
- `AUTH_PORT`
- `CHAT_PORT`
- `GATEWAY_PORT`
- `QUERY_SERVICE_URL`
- `LLM_SERVICE_URL`
- `REPORTS_SERVICE_URL`
- `META_SERVICE_URL`
- `AUTH_SERVICE_URL`
- `CHAT_SERVICE_URL`
- `ROOT_EMAIL`
- `ROOT_PASSWORD`
- `ROOT_FULL_NAME`
- `ROOT_DEPARTMENT`
- `PASSWORD_SALT`
- `LLM_PROVIDER`
- `LLM_SETTINGS_FILE`
- `CEREBRAS_API_KEY`
- `CEREBRAS_MODEL`
- `CEREBRAS_CHAT_URL`
- `APP_TIMEZONE`

Файл-образец:

- `.env.example`

## 10. Как добавить новую метрику

Минимальный набор шагов:

1. Добавить определение метрики в `internal/shared/contracts.go` в `DefaultSemanticLayer()`.
2. Добавить формулу и примеры использования в `config/qwen_sql_settings.md`.
3. Если нужна новая колонка или витрина — расширить allowlist в `internal/shared/llm.go`.
4. При необходимости обновить описания/синонимы/термины в `app.semantic_terms` или seed.
5. Добавить/обновить тесты guardrails.

Если метрика требует новых колонок, сначала нужно расширить слой данных в `db/schema.sql`.

## 11. Как добавить новое измерение или фильтр

### Новое измерение

1. Добавить `DimensionDefinition` в `DefaultSemanticLayer()`.
2. Описать колонку и правила группировки в `config/qwen_sql_settings.md`.
3. Разрешить колонку/витрину в allowlist `internal/shared/llm.go`.
4. Добавить тест guardrails на безопасный SQL с новым измерением.

### Новый фильтр

1. Убедиться, что колонка есть в аналитическом слое.
2. Добавить поле в allowlist `internal/shared/llm.go`.
3. Обновить semantic layer и `config/qwen_sql_settings.md`.
4. Добавить тесты на безопасную фильтрацию.

## 12. Как добавить новый экран во frontend

1. Создать новый `.html` в `web/`.
2. Создать соответствующий `.js`.
3. Добавить навигационную ссылку в нужные страницы/сайдбар.
4. Убедиться, что `gateway` раздаёт страницу из `web/`.

Так как `gateway` отдаёт статические файлы напрямую, отдельной сборки frontend сейчас не требуется.

## 13. Как добавить новый backend endpoint

1. Определить сервис, которому принадлежит бизнес-логика.
2. Добавить handler в `cmd/<service>/main.go` либо в выделенный handler/store файл.
3. При необходимости расширить shared-контракты.
4. Если endpoint должен быть доступен из браузера, прокинуть маршрут через `gateway`.
5. Добавить фронтенд-вызов.

Если API защищённый, доступ к нему обычно должен идти через `authenticatedProxy` в `gateway`.

## 14. Безопасность и архитектурные ограничения

Проект держится на нескольких принципах, их важно не сломать:

### LLM не должен выполнять SQL

Правильный путь:

- текст -> intent -> локальный SQL guardrails -> validation -> execute

Неправильный путь:

- текст -> LLM -> raw SQL -> execute

### Query execution должен идти под read-only пользователем

Для этого используется `PG_READONLY_DSN`.

Даже если логика guardrails где-то даст сбой, read-only пользователь снижает риск повреждения данных.

### Фильтры и группировки должны быть allowlist-based

Нельзя открывать произвольные поля, переданные из LLM или пользователя, без явного whitelist.

### Shared contracts должны оставаться единым источником форматов

Не нужно дублировать структуры ответа в каждом сервисе отдельно.

## 15. Тесты

Запуск:

```powershell
go test ./...
```

По проекту тесты есть у:

- `cmd/llm`
- `cmd/query`
- `cmd/reports`
- `internal/shared`

## 16. Документы в `docs/`

- `docs/architecture.md` — концептуальная архитектура
- `docs/implementation-plan.md` — план реализации

Они полезны как бизнес- и проектные артефакты, но технической истиной по поведению системы всё равно остаётся код.

## 17. Практическая карта файлов

Ниже краткая карта, где искать нужную ответственность.

### Если ломается логин

Смотреть:

- `cmd/auth/main.go`
- `internal/shared/auth.go`
- `internal/shared/http.go`
- таблицы `app.users`, `app.user_sessions`

### Если неправильно понимается вопрос

Смотреть:

- `config/qwen_sql_settings.md`
- `cmd/llm/provider.go`
- `cmd/llm/qwen.go`
- `internal/shared/contracts.go`

### Если строится неверный SQL

Смотреть:

- `internal/shared/llm.go`
- `cmd/query/main.go`
- `cmd/query/plan.go`

### Если отсутствуют метрики/термины/группировки

Смотреть:

- `internal/shared/contracts.go`
- `cmd/meta/main.go`
- `db/schema.sql`
- `db/seed.sql`
- `app.semantic_terms`

### Если проблемы с отчётами и шаблонами

Смотреть:

- `cmd/reports/main.go`
- `cmd/reports/report_handlers.go`
- `cmd/reports/report_store.go`
- `cmd/reports/template_handlers.go`
- `cmd/reports/template_store.go`
- `cmd/reports/scheduler.go`

### Если проблемы с чатами

Смотреть:

- `cmd/chat/main.go`
- таблицы `app.chat_rooms`, `app.chat_room_members`, `app.chat_messages`

### Если не открываются страницы

Смотреть:

- `cmd/gateway/main.go`
- `web/*.html`
- `web/*.js`
- `web/styles.css`

## 18. Рекомендации по развитию

Логичные направления развития проекта:

- полноценный RBAC вместо текущей упрощённой модели;
- кеширование semantic layer и результатов;
- materialized views для тяжёлых сценариев;
- асинхронные фоновые задачи и очередь;
- observability: метрики, tracing, structured logs;
- более строгая миграционная стратегия;
- расширение набора аналитических метрик;
- multi-tenant или более развитая модель department scoping;
- вынесение chat pub/sub в отдельный транспорт, если появится горизонтальное масштабирование.

## 19. Итог

Проект уже построен вокруг хорошей инженерной идеи: детерминированный backend контролирует аналитику, а LLM используется только как слой понимания языка. При дальнейшем развитии важно сохранять это разделение ответственности, потому что именно оно делает систему одновременно полезной, объяснимой и безопасной.
