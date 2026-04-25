# Implementation plan

## Текущая LLM-цепочка

1. Пользовательский текст приходит в `query`.
2. `query` получает semantic layer из `meta`.
3. `query` отправляет текст и semantic layer в `llm`.
4. `llm` на каждый запрос читает `config/qwen_sql_settings.md`.
5. `llm` вызывает Qwen через Cerebras Cloud и получает JSON с SQL, intent, clarification и confidence.
6. `query` прогоняет SQL через guardrails.
7. SQL выполняется под `PG_READONLY_DSN`.

## Что важно поддерживать

- Не возвращать в цепочку старый внешний провайдер.
- Не возвращать локальный словарный parser как источник SQL/intent.
- Все изменения БД доступны модели только через описание в `config/qwen_sql_settings.md`.
- Любое расширение таблиц должно сопровождаться обновлением allowlist в `internal/shared/llm.go` и описания в `config/qwen_sql_settings.md`.
