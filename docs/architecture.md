# Архитектура Drivee Analytics MVP

## Цель

Собрать self-service аналитическую платформу, в которой бизнес-пользователь задаёт вопрос по-русски и получает воспроизводимый аналитический результат без знания SQL.

## Контур решения

1. `Gateway` отдаёт frontend и проксирует запросы во внутренние сервисы.
2. `Meta Service` публикует разрешённые метрики, формулы, синонимы, измерения, ограничения по разрезам, словарь терминов и шаблоны вопросов.
3. `LLM Service` читает файл `config/qwen_sql_settings.md`, отправляет русский текст и semantic layer в Qwen через Cerebras Cloud и получает JSON с SQL, intent и пояснениями.
4. `Query Service` не доверяет SQL модели напрямую: валидирует его через guardrails, проверяет источники/операции/колонки/лимиты и только потом исполняет.
5. `Reports Service` сохраняет отчёты, историю запусков и переиспользует готовые сценарии.
6. `PostgreSQL` хранит аналитический факт и служебные таблицы приложения; выполнение аналитического SQL идёт под отдельной read-only ролью.

## Backend pipeline

1. Пользователь отправляет текстовый запрос в `Gateway`.
2. `Query Service` запрашивает актуальный semantic layer у `Meta Service`.
3. `Query Service` отправляет текст и semantic layer в `LLM Service`.
4. `LLM Service` на каждый запрос заново подтягивает `LLM_SETTINGS_FILE`, где описаны разрешённые таблицы, колонки, бизнес-смысл полей и строгие правила генерации SQL.
5. `LLM Service` вызывает Qwen: `qwen-3-235b-a22b-instruct-2507` через Cerebras Chat Completions.
6. Qwen возвращает JSON: `sql`, `intent`, `clarification`, `confidence`.
7. `Query Service` запускает `ValidateGeneratedSQL`: разрешены только безопасные `SELECT`/`WITH`, источники из allowlist и явные колонки.
8. SQL выполняется через `PG_READONLY_DSN` под защищённым пользователем без доступа к схеме `app`.
9. Результат логируется, обогащается chart-метаданными и возвращается во frontend.

## Guardrails

- Qwen никогда не получает прямой доступ к БД.
- Старый гибридный parser удалён из цепочки обработки запроса.
- SQL приходит от Qwen, но не исполняется без проверки backend guardrails.
- Поддерживается только один read-only запрос без `;` и комментариев.
- Заблокированы DDL/DML/служебные схемы PostgreSQL, `SELECT *`, опасные функции и неизвестные источники.
- Разрешённые источники: `analytics.incity`, `analytics.v_incity_orders_latest`, `analytics.v_ride_metrics`, `analytics.v_driver_daily_metrics`, `analytics.v_passenger_daily_metrics`.
- В БД предусмотрена отдельная read-only роль `analytics_readonly`.
- Все запросы и ошибки журналируются в `app.query_logs`.

## Конфигурация LLM

Основной файл: `config/qwen_sql_settings.md`.

Он содержит:

- список разрешённых таблиц;
- список колонок;
- описание бизнес-смысла каждой колонки;
- правила выбора витрины;
- правила относительных периодов;
- формат JSON-ответа;
- запреты безопасности;
- примеры SQL.

Ключевые переменные окружения:

- `CEREBRAS_API_KEY` — ключ из Cerebras Cloud;
- `CEREBRAS_MODEL` — `qwen-3-235b-a22b-instruct-2507`;
- `CEREBRAS_CHAT_URL` — `https://api.cerebras.ai/v1/chat/completions`;
- `LLM_SETTINGS_FILE` — путь к файлу настроек нейросети.
