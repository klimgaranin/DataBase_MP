# PROJECT_CONTEXT

## Назначение

`DataBase_MP` собирает и хранит данные Wildberries в локальной PostgreSQL:

- заказы WB Statistics API;
- остатки WB Analytics API;
- рекламные кампании WB Advertising API;
- fullstats рекламных кампаний с записью в PostgreSQL и Google Sheets.

Проект ориентирован на Windows-запуск через `.cmd` scripts и Планировщик задач.
PostgreSQL запускается через Docker Compose.

## Стек

- Python 3.11+.
- PostgreSQL 16 в Docker.
- `requests`, `psycopg2-binary`, `python-dotenv`.
- Для API server: FastAPI/Pydantic/Uvicorn используются в коде, но зависимости
  требуют сверки с текущим `requirements.txt`.
- Для Google Sheets в `job_wb_adv_fullstats.py` используется `gspread`; наличие
  в `requirements.txt` требует проверки.

## Основная структура

- `app/clients/` — HTTP-клиенты WB API.
- `app/jobs/` — запускаемые ETL-джобы.
- `app/jobs/wb_orders_logic.py` — чистая проверяемая логика orders job:
  lookback, dedupe, расчёт cursor.
- `app/normalize/` — нормализация ответов API в плоские структуры БД/Sheets.
- `app/db.py` — подключение к PostgreSQL и операции чтения/записи.
- `app/utils.py` — `.env`, `sys.path`, логирование, время, Telegram.
- `app/api_server.py` — FastAPI HTTP-триггер для fullstats.
- `migrations/` — SQL-миграции схемы.
- `infra/docker-compose.yml` — PostgreSQL 16.
- `scripts/` — Windows entrypoints для Task Scheduler.
- `tools/` — вспомогательные review/export scripts.
- `tools/project_audit.py` — проверка целостности проекта без WB/DB-запусков.
- `tools/project_standards_check.py` — проверка наличия проектных стандартов и
  отсутствия очевидного мусора в корне.
- `tools/health_check.py` — диагностика окружения/зависимостей/БД без WB API.
- `tools/sheets_audit.py` — read-only аудит Google Таблицы `Аналитика МП`:
  листы, заголовки, формулы, `IMPORTRANGE`, примеры строк.
- `tools/source_file_audit.py` — read-only аудит локальных CSV/XLSX/XLSM
  выгрузок.
- `tools/xlsm_powerquery_audit.py` — read-only аудит Excel Power Query/VBA:
  источники, таблицы, подключения, M-код с маскировкой секретов и макросы без
  выполнения.
- `.codex/AI_WORKFLOW.md` — локальный рабочий процесс по мотивам gstack:
  продуктовая постановка, архитектура, ETL-review, DB-safety, UI-review,
  release-check.
- `.codex/PROJECT_STANDARDS.md` — стандарты структуры проекта, типизации,
  именования, БД-слоёв, миграций и уборки временных файлов.
- `.codex/roles/` — проектные роли Codex для системной разработки.
- `.codex/workflows/` — чеклисты feature intake, change review, release check
  и cleanup check.
- `archive/` — локальные архивы разовых задач, игнорируются git.
- `local/` — локальные артефакты аудита/разработки, игнорируются git.

## Production jobs

### `wb_orders`

Файл: `app/jobs/job_wb_orders.py`

- Источник: `statistics-api.wildberries.ru/api/v1/supplier/orders`.
- Токен: `WB_TOKEN`.
- Курсор: `job_cursors`, key `wb_orders`.
- Защита от пропусков: lookback от курсора.
- Raw dedup: `wb_orders_raw_dedup`.
- Norm: `wb_orders_norm`.
- Lock: advisory lock `4242001`.
- Запуск: `scripts/run_wb_orders.cmd`.

### `wb_stocks`

Файл: `app/jobs/job_wb_stocks.py`

- Источник: `seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses`.
- Токен: `WB_TOKEN` с доступом к аналитике.
- Raw snapshots: `wb_stocks_raw`.
- Current snapshot: `wb_stocks_snap`.
- Lock: advisory lock `4242002`.
- Запуск: `scripts/run_wb_stocks.cmd`.

### `wb_adv_campaigns`

Файл: `app/jobs/job_wb_adv_campaigns.py`

- Источник: WB Advertising `/adv/v1/promotion/count`.
- Токен: `WB_TOKEN_CONTENT`.
- Таблица: `wb_adv_campaigns`.
- Lock: advisory lock `4242003`.
- Запуск: `scripts/run_wb_adv_campaigns.cmd`.
- Статус на 2026-07-21: модуль временно вынесен за рамки ближайшего
  рефакторинга. Не архивировать, но не трогать без необходимости.

### `wb_adv_fullstats`

Файл: `app/jobs/job_wb_adv_fullstats.py`

- Источник: WB Advertising `/adv/v3/fullstats`.
- Токен: `WB_TOKEN_CONTENT`.
- Берёт кампании из `wb_adv_campaigns` со статусами `7`, `9`, `11`.
- Пишет raw/norm в PostgreSQL через `atomic_save_fullstats`.
- Пишет агрегированные строки в Google Sheets через `gspread`.
- Lock: advisory lock `4242004`.
- Может запускаться через `app/api_server.py`.
- Статус на 2026-07-21: модуль специфически настроен через API/GAS trigger и
  пока не горит. Не рефакторить, если это не требуется для общей работы проекта.

### `source_statistics`

Файл: `app/jobs/job_source_statistics.py`

- Источник: материализованные Excel tables из
  `local/source_exports/Статистика.xlsm`.
- По умолчанию пропускает WB-блоки, потому что WB заказы и остатки уже
  загружаются jobs `wb_orders` и `wb_stocks`.
- По умолчанию загружает внутренние файловые источники: остатки 1С и список
  заказов. WB уже грузится production jobs, Ozon переводится на API, Яндекс
  пока закрыт.
- Windows-серверные файловые источники:
  список заказов `\\tsclient\P\Список заказов` (`.xlsx`), остатки 1С
  `\\tsclient\S\МП` (`.txt` или `.xls`).
- Raw snapshots: `raw.source_file_snapshots`.
- Staging/current tables:
  `staging.source_orders_daily`, `staging.source_stock_summary`,
  `staging.ozon_storage_costs`, `core.production_inventory_snapshot`,
  `staging.supply_pipeline_current`.
- Lock: advisory lock `4242101`.
- Запуск: `scripts/run_source_statistics.cmd`.

### `ozon_orders`

Файл: `app/jobs/job_ozon_orders.py`

- Источник: Ozon Seller API `POST /v3/posting/fbo/list`.
- Секреты: `OZON_CLIENT_ID`, `OZON_API_KEY`; целевой backend — keyring.
- Первый запуск: с 1 января текущего года.
- Дальше: актуализация по `job_cursors` с lookback, плановое расписание каждый
  час.
- Raw HTTP logs: `raw.api_responses`.
- Raw postings: `raw.ozon_fbo_postings`.
- Posting change history: `raw.ozon_fbo_posting_versions`.
- Full technical items: `staging.ozon_fbo_order_items_full`.
- Analytics view: `analytics.ozon_fbo_order_items_flat`.
- Change history view: `analytics.ozon_fbo_posting_change_history_flat`.
- Lock: advisory lock `4242201`.
- Запуск: `scripts/run_ozon_orders.cmd`.
- Регистрация в Windows Task Scheduler:
  `scripts/register_ozon_orders_task.ps1`.

## Приоритет ближайшей работы

Первый бизнес-модуль будущей системы — `Аналитика МП`: отдельно WB и отдельно
Ozon, по аналогии с живой Google Таблицей `Аналитика МП` из проекта
`mp-gas/apps/analytics-mp`.

Google Таблица является бизнес-эталоном на старте. Важно смотреть не только
GAS-код, но и сами листы, формулы, скрытый `DATA`, `IMPORTRANGE`, ручные поля и
логику листов `Аналитика WB`, `Аналитика OZON`, `ПАНЕЛЬ`, `Остатки ЧЗ`,
`ИНФО`, `БД`, `Stats_Full_WB`.

WB заказы и остатки уже загружаются действующими jobs в PostgreSQL:
`wb_orders_norm`, `wb_orders_raw_dedup`, `wb_stocks_snap`, `wb_stocks_raw`.
Excel/Google Sheets для WB использовать как бизнес-эталон расчётов, но не как
повод дублировать уже работающие выгрузки.

Локальный файл `local/source_exports/Статистика.xlsm` разобран статически:
он содержит 12 Power Query и VBA-обвязку для запуска WB заказов по 3 месячным
запросам с паузами. Для будущей системы из него важны Ozon/Yandex/file-based
источники, внутренние остатки/производственные статусы и бизнес-агрегации.
Секреты, найденные в Power Query, не переносить в код или отчёты.

Пользователь уточнил Windows-пути файловых источников: список заказов лежит в
`\\tsclient\P\Список заказов`, остатки 1С — в `\\tsclient\S\МП`.

Ozon API расписание целевого прототипа:

- `ozon_orders` — каждый час, первый запуск с начала года;
- `ozon_placement` — один раз утром;
- `ozon_stocks` — два раза в день.

Рекламный блок (`wb_adv_*`, `api_server`, GAS-trigger) оставлять в рабочем
состоянии, но не рефакторить первым, если он не мешает общей работе.

## База данных

Миграции:

- `V1__init_raw.sql`
- `V2__wb_orders_norm.sql`
- `V3__raw_retention_index.sql`
- `V4__job_state.sql`
- `V5__wb_orders_raw_dedup.sql`
- `V6__job_runs.sql`
- `V7__job_runs_dups.sql`
- `V8__wb_adv_tables.sql`
- `V9__wb_adv_fullstats_norm_extend.sql`
- `V10__marketplace_analytics_foundation.sql`
- `V11__source_statistics_ingestion.sql`
- `V12__ozon_api_orders.sql`
- `V13__ozon_stocks_and_placement.sql`
- `V14__ozon_stock_details_and_fbo_payload.sql`
- `V15__ozon_order_items_flat_columns.sql`
- `V16__ozon_stocks_products_flat_columns.sql`
- `V17__ozon_stock_details_backfill_from_raw.sql`
- `V18__ozon_latest_stock_flat_view.sql`
- `V19__ozon_placement_cells.sql`
- `V20__ozon_fbo_orders_full_technical_table.sql`
- `V21__ozon_fbo_posting_change_history.sql`

Ключевые таблицы:

- `wb_orders_raw_dedup`
- `wb_orders_norm`
- `wb_stocks_raw`
- `wb_stocks_snap`
- `wb_adv_campaigns`
- `wb_adv_fullstats_raw`
- `wb_adv_fullstats_norm`
- `job_cursors`
- `job_runs`
- `raw.api_responses`
- `raw.pipeline_runs`
- `core.products`
- `core.marketplace_products`
- `staging.marketplace_stock_locations`
- `staging.marketplace_stock_summary`
- `raw.source_file_snapshots`
- `raw.ozon_fbo_postings`
- `raw.ozon_fbo_posting_versions`
- `raw.ozon_product_list_items`
- `raw.ozon_product_info_items`
- `raw.ozon_analytics_stocks`
- `raw.ozon_placement_reports`
- `staging.source_orders_daily`
- `staging.ozon_fbo_order_items_full`
- `staging.ozon_stock_details`
- `staging.ozon_stock_by_cluster`
- `staging.ozon_placement_by_products`
- `staging.ozon_placement_cells`
- `staging.source_stock_summary`
- `staging.ozon_storage_costs`
- `staging.supply_pipeline_current`
- `analytics.marketplace_current`
- `analytics.ozon_fbo_order_items_flat`
- `analytics.ozon_products_flat`
- `analytics.ozon_stock_details_flat`
- `analytics.ozon_stock_details_latest_flat`
- `analytics.ozon_placement_cells_flat`

## Конфигурация и секреты

Основной файл локальных секретов: `.env` в корне проекта.

В git должен попадать только `.env.example`.

Новый переходный слой секретов: `app/secrets.py`. Он поддерживает:

- `APP_SECRET_BACKEND=env` — совместимость со старым `.env`;
- `APP_SECRET_BACKEND=keyring` — Windows Credential Manager через `keyring`.

Админские проверки должны показывать только наличие секрета, но не значение.

Секреты:

- `WB_TOKEN`
- `WB_TOKEN_CONTENT`
- `PG_DSN`
- `POSTGRES_PASSWORD`
- `TG_BOT_TOKEN`
- `TG_CHAT_ID`
- `API_SERVER_TOKEN`
- `GSPREAD_SA_FILE`

Не выводить значения секретов в ответы и логи.

## Архив WB legal export

Разовая доказательная выгрузка WB от 2026-07-18 вынесена в:

`archive/wb-legal-export-20260718/`

Финальный evidence-пакет внутри архива:

`archive/wb-legal-export-20260718/evidence/wb-legal-export-20260718-182750/`

Это не активный ETL-код. Использовать только как архив/референс.

## Известные технические риски

- Advertising/API/GAS зависимости пока не сверялись, потому что модуль
  временно вне ближайшего scope.
- README частично отстаёт от фактической структуры.
- В `app/db.py` смешаны DDL и runtime-операции; часть схемы также живёт в
  migrations.
- Есть первый тестовый каркас для core-нормализации и orders helper-логики, но
  полноценного набора тестов для production ETL пока нет.
- В `Статистика.xlsm` есть вшитые API-секреты в Power Query. Аудит их
  маскирует; целевое хранение — `app/secrets.py`/keyring.
- Боевые WB-запросы из WSL могут не проходить напрямую; использовать
  Windows-мост.
- На 2026-07-21 текущий WSL `.env` содержит `PG_DSN` в libpq-формате с ошибкой
  кавычек; `tools/health_check.py` показывает это как `No closing quotation`.
  Значение секрета не фиксируется в документации.
- В WSL доступен Python 3.14, а проектная документация ориентируется на
  Python 3.11+. `psycopg2-binary==2.9.9` не установился в WSL Python 3.14 без
  `pg_config`; для боевых проверок использовать Windows Python 3.11 или
  согласовать установку системных dev-пакетов.
