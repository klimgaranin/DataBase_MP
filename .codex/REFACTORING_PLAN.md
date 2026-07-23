# REFACTORING_PLAN

## Цель

Подготовить `DataBase_MP` к стабильной долгосрочной поддержке без риска
сломать текущие Windows scheduled jobs.

## Принципы

- Рефакторить по шагам, каждый шаг проверяемый.
- Сначала покрыть тестами нормализацию и job boundaries, потом менять общие
  модули.
- Не смешивать cleanup, архитектурные изменения и бизнес-логику в одном PR/коммите.
- Production jobs должны сохранять текущие entrypoints и exit codes.
- Рекламный блок `wb_adv_*`, `api_server` и GAS-trigger пока не рефакторить:
  он рабочий, специфически настроенный и временно вне ближайшего scope.

## Этап 1. Инвентаризация и зависимости

Задачи:

- Нормализовать кодировку `requirements.txt`. Статус: сделано для core deps.
- Сверить core-зависимости:
  - `psycopg2-binary`
  - `python-dotenv`
  - `requests`
- Зафиксировать advertising/API/GAS зависимости отдельно, но не менять их
  сейчас без отдельной задачи.
- Обновить README под core jobs: orders, stocks, PostgreSQL, Windows scripts.
- Зафиксировать команды Windows-запуска и smoke-checks.

Риск: низкий.

## Этап 2. Тестовый каркас

Задачи:

- Добавить unit-тесты нормализации:
  - `norm_wb_orders`
  - `norm_wb_stocks`
- Добавить тесты конфигурации HTTP-клиентов без сетевых запросов.
- Добавить тесты расчёта lookback/cursor для `job_wb_orders`.
- Вынести orders helper-логику отдельно от entrypoint. Статус: сделано,
  `app/jobs/wb_orders_logic.py`.
- Добавить smoke import tests для core entrypoints. Advertising entrypoints
  отдельно, когда модуль вернётся в scope.

Риск: низкий.

## Этап 3. DB слой

Задачи:

- Разделить `app/db.py` на:
  - `app/db/connection.py`
  - `app/db/job_state.py`
  - `app/db/orders_repo.py`
  - `app/db/stocks_repo.py`
- `app/db/adv_repo.py`
- Оставить обратимые thin wrappers в `app/db.py` на переходный период, чтобы
  старые imports не сломались.
- Перенести новые DDL-изменения только в `migrations/`.

Риск: средний, потому что `db.py` используется всеми jobs.

## Этап 4. Общая job-обвязка

Задачи:

- Вынести повторяющийся код:
  - advisory lock acquire/release;
  - `job_runs`;
  - Telegram status;
  - стандартное логирование.
- Сделать общий helper без изменения поведения existing jobs.
- Подключать jobs по одному, начиная с `wb_orders` и `wb_stocks`.

Риск: средний.

## Этап 5. HTTP-клиенты WB

Задачи:

- Унифицировать config helpers `_env_bool/_env_int/_env_float`.
- Унифицировать retry/backoff/rate-limit.
- Добавить безопасное логирование запросов без токенов для debug-режима.
- Не смешивать разные WB domains в один большой client; держать клиенты по API:
  Statistics, Analytics, Advertising.

Риск: средний.

## Этап 6. Advertising/API server

Задачи:

- Выполнять только после отдельного решения вернуть рекламный модуль в scope.
- Сверить зависимости FastAPI/Pydantic/Uvicorn.
- Добавить health/smoke tests.
- Зафиксировать правило `workers=1`, потому что `_active_job` хранится in-memory.
- Проверить Windows `.cmd` запуск `scripts/run_api_server.cmd`.

Риск: низкий-средний.

## Этап 7. Эксплуатационная документация

Задачи:

- Обновить README.
- Добавить команды проверки БД, курсоров и последних запусков.
- Описать восстановление после падения каждого job.
- Описать, какие токены WB нужны для каждой секции.

Риск: низкий.

## Не делать первым

- Не переписывать все jobs на новый framework одним изменением.
- Не менять схему БД без миграции.
- Не трогать `.env` автоматически.
- Не удалять `archive/wb-legal-export-20260718/`.
