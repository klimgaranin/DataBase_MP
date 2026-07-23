# TASKS

## Текущий статус

- Основные production jobs сохранены в активном коде.
- Разовая WB legal export задача заархивирована в
  `archive/wb-legal-export-20260718/`.
- Контекстные файлы проекта созданы для будущих Codex-сессий.

## Активные задачи

### 1. Подготовить проект к полноценной поддержке

Статус: в работе.

Что уже сделано:

- Добавлен `AGENTS.md`.
- Добавлены `.codex/PROJECT_CONTEXT.md`, `.codex/TASKS.md`, `.codex/DECISIONS.md`.
- Добавлен локальный AI workflow по мотивам gstack:
  `.codex/AI_WORKFLOW.md`, роли в `.codex/roles/`, чеклисты в
  `.codex/workflows/`, шаблон `.codex/templates/TASK_BRIEF.md`.
- Добавлены стандарты разработки `.codex/PROJECT_STANDARDS.md`: структура
  папок, именование Python/БД, типизация, миграции, jobs, временные файлы и
  обязательная уборка после задач.
- Добавлены `roles/project_steward.md`, `workflows/cleanup_check.md`,
  `tools/project_standards_check.py` и тест
  `tests/test_project_standards_check.py`.
- Добавлен `tools/ai_workflow_check.py` и тест `tests/test_ai_workflow_check.py`,
  чтобы проверять наличие этих инструкций.
- Task-specific WB legal export вынесен в архив.
- `requirements.txt` нормализован в UTF-8 без изменения core-версий.
- Добавлены первые unit-тесты для core-нормализации orders/stocks и helper
  функций `job_wb_orders`.
- Чистая логика orders вынесена в `app/jobs/wb_orders_logic.py`.
- Добавлены `tools/project_audit.py` и `tools/health_check.py`.
- Windows-проверка через мост проходит для tests/compileall/project audit.
- Добавлен `tools/sheets_audit.py` для read-only аудита Google Таблицы
  `Аналитика МП`.
- Добавлен переходный слой секретов `app/secrets.py` с поддержкой `keyring`.
- Добавлен каталог ключевых WB/Ozon API-методов `app/integrations/api_catalog.py`
  на базе swagger/YAML из `mp-gas/shared`.
- Выполнен read-only аудит таблицы `Аналитика МП`; локальный результат:
  `local/audits/sheets/analytics_mp_sheets_audit_20260721-203226.md`.
- Добавлен первый документ логики `.codex/ANALYTICS_MP_LOGIC.md`.
- Добавлена миграция `V10__marketplace_analytics_foundation.sql` с отдельными
  схемами `raw`, `staging`, `core`, `analytics` и первыми таблицами аналитики.
- Добавлен `tools/sheets_logic_map.py`, который строит карту формул Google
  Таблицы по категориям будущих БД-слоёв.
- Добавлен `tools/source_file_audit.py` для локального `.xlsx`/`.csv` файла
  выгрузки API endpoints.
- Добавлен `tools/xlsm_powerquery_audit.py` и разобран
  `local/source_exports/Статистика.xlsm`: 12 Power Query, 12 Excel tables,
  VBA-обвязка для WB заказов.
- Добавлен первый job `app/jobs/job_source_statistics.py` по форме WB jobs:
  читает материализованные Excel tables из `Статистика.xlsm`, по умолчанию
  пропускает WB-блоки, сохраняет raw snapshot и staging для Ozon/Yandex/
  внутренних источников.
- Добавлена миграция `V11__source_statistics_ingestion.sql`.
- Добавлен первый Ozon API job `app/jobs/job_ozon_orders.py`:
  `/v3/posting/fbo/list`, первый запуск с 1 января текущего года, дальнейшая
  почасовая актуализация через `job_cursors`, raw HTTP logs, raw postings и
  staging order items. После сверки со swagger исправлен v3 payload: фильтр
  только по периоду и полному списку FBO-статусов, без `posting_numbers`;
  ответ читается из верхнего уровня `postings/cursor/has_next`.
- Добавлена миграция `V12__ozon_api_orders.sql`.
- Добавлены Ozon API jobs `app/jobs/job_ozon_stocks.py` и
  `app/jobs/job_ozon_placement.py`: остатки Ozon через product list/info +
  analytics stocks, стоимость размещения через асинхронный XLSX-отчёт.
- Добавлена миграция `V13__ozon_stocks_and_placement.sql`.
- `job_source_statistics.py` переведён на прямые Windows-файлы для внутренних
  источников: список заказов `.xlsx`, остатки 1С `.txt` или `.xls`; при
  недоступности путей остаётся fallback на `Статистика.xlsm`.
- Найден рабочий способ запуска из WSL-клона на боевом сервере через
  Windows-мост. Добавлены `tools/sync_local_postgres_password.py` и
  `tools/apply_migrations.py`.
- V10-V13 применены к локальной PostgreSQL. Боевой `source_statistics` прошёл:
  1118 строк остатков 1С, 347 строк списка заказов, 1465 строк к записи.
- После сравнения с GAS Ozon stocks переведён на batch-запросы к
  `/v1/analytics/stocks` с retry по 429/5xx. Боевой запуск `ozon_stocks`
  прошёл: 1144 товаров Ozon, 5912 строк analytics stocks в последнем raw
  снимке, текущая staging-витрина `staging.ozon_stock_by_cluster` содержит
  5346 строк и сумму `available_stock_count = 50853`.
- `raw.ozon_analytics_stocks` хранит историю снимков. Если суммировать весь
  raw без фильтра по `source_run_id`, остатки завышаются. Добавлена V14:
  `staging.ozon_stock_details` для полных нормализованных строк без
  суммирования, `staging.ozon_stock_by_cluster` остаётся агрегированной текущей
  витриной.
- Ozon FBO orders приведён к чистому техническому правилу: `raw.ozon_fbo_postings`
  остаётся сырой страховкой, `staging.ozon_fbo_order_items_full` хранит полную
  нормализованную строку товара с раскрытой шапкой отправления, `products[]`,
  `financial_data.products[]`, `analytics_data`, `external_order`,
  `additional_data`, комиссиями и скидками. Старые укороченные staging-таблицы
  `staging.ozon_fbo_posting_details`, `staging.ozon_order_item_details`,
  `staging.ozon_order_items` удалены миграцией V20. Удобная view
  `analytics.ozon_fbo_order_items_flat` теперь строится поверх полной
  технической таблицы.
- Для Ozon FBO orders добавлена история изменений по отправлению:
  `raw.ozon_fbo_posting_versions` хранит новую версию только если полный
  JSONB payload по `posting_number` реально изменился. Для просмотра статусов и
  изменений добавлена view `analytics.ozon_fbo_posting_change_history_flat`.
- Добавлен Windows-скрипт регистрации регулярной задачи Ozon FBO orders:
  `scripts/register_ozon_orders_task.ps1`. По умолчанию создаёт/обновляет
  `\MarketplaceDB\Ozon_Orders_Sync` с запуском каждый час.
- Ozon stocks/products приведены к тому же правилу: product list/info и
  analytics stocks раскрыты в колонки, из сохранённого raw восстановлена
  `staging.ozon_stock_details`, добавлены плоские views
  `analytics.ozon_products_flat`, `analytics.ozon_stock_details_flat` и
  `analytics.ozon_stock_details_latest_flat`. Текущая контрольная сумма
  последнего снимка Ozon остатков: `available_stock_count = 50853`.
- Для Ozon placement добавлена `staging.ozon_placement_cells` и
  `analytics.ozon_placement_cells_flat`: переменные оригинальные колонки Excel-
  отчёта сохраняются как ячейки `row_number + column_name + value`, чтобы не
  доставать их вручную из `payload`.
- `tools/health_check.py` теперь работает с текущим форматом `PG_DSN` и
  локальным `POSTGRES_PASSWORD`; WSL `.venv` дополнен `psycopg2-binary`.
- Исправлен общий advisory lock в `app/db.py`: lock теперь держится на живом
  PostgreSQL-соединении до `advisory_unlock`. Старые Docker warnings
  `you don't own a lock of type ExclusiveLock` были вызваны прежней реализацией,
  где lock брался и отпускался на разных соединениях.

Следующие шаги:

- Расширить smoke/unit тесты для orders/stocks и общих helper-функций без
  реальных WB-запросов.
- Использовать действующие WB jobs как источник заказов/остатков в будущих
  витринах, не делать вторую параллельную WB-загрузку из логики XLSM.
- Следующий боевой запуск Ozon FBO orders должен подтвердить почасовую
  актуализацию: повтор без изменений не должен раздувать
  `raw.ozon_fbo_posting_versions`, а изменённый статус должен дать новую
  версию.
- Выполнить боевой запуск `scripts/run_ozon_placement.cmd` после Ozon stocks/
  orders проверки.
- Выполнить повторный боевой запуск `scripts/run_source_statistics.cmd` уже из
  production Windows-папки после pull/деплоя.
- После боевой проверки добавить расписание: Ozon placement утром, Ozon stocks
  два раза в день.
- Спроектировать первые витрины `analytics_wb_current` и
  `analytics_ozon_current` отдельно, без смешивания маркетплейсов.
- Исправить локальный `PG_DSN` в `.env` по инструкции пользователя, если нужно
  подключаться к БД из этой WSL-папки.
- Обновить README сначала под core jobs (`orders`, `stocks`, PostgreSQL,
  Windows scripts).

### Advertising/fullstats временно вне ближайшего рефакторинга

Статус: заморожено.

Модуль `wb_adv_campaigns`, `wb_adv_fullstats`, `api_server` и связанный
GAS-trigger не архивировать и не удалять. Не менять, пока это не требуется для
общей работы проекта или отдельной задачи пользователя.

### 2. Рефакторинг архитектуры ETL

Статус: планируется.

Цель: снизить риск поломки действующих jobs и упростить развитие.

Основные направления:

- Разделить DB DDL, repositories и runtime job state.
- Унифицировать HTTP-клиенты WB по retry/rate-limit/config, начиная с
  orders/stocks.
- Вынести общие job-обвязки: advisory lock, job_runs, TG alerts.
- Добавить тесты на нормализацию и DB SQL boundaries.

### 3. Windows-first эксплуатация

Статус: постоянно актуально.

- Проверять `.cmd` scripts после изменений entrypoints.
- Не считать WSL-only результат достаточным для боевых WB-интеграций.
- При сетевых проблемах использовать Windows-мост.

## Завершённые задачи

### WB legal export 2026-07-18

Статус: завершено и заархивировано.

Результат:

- Финальный evidence-пакет сохранён.
- Код и тесты разовой задачи вынесены из активного `app/`.
- Архив: `archive/wb-legal-export-20260718/`.

## Проверки

Базовые проверки после изменений:

```bash
python -m compileall app
```

Если есть тесты:

```bash
python -m unittest discover -s tests
```

Проверка локального AI workflow:

```bash
python tools/ai_workflow_check.py
python tools/project_standards_check.py
```

Для production jobs дополнительно проверять Windows-запуск соответствующего
`.cmd` или Python entrypoint через Windows-мост.
