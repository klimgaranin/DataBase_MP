# DataBase_MP — Автоматизация загрузки данных маркетплейсов

Проект автоматически собирает данные с Wildberries в локальную PostgreSQL-базу через Docker.
Данные обновляются по расписанию через Планировщик задач Windows.

Текущий основной фокус проекта: стабильные core-джобы заказов и остатков.
Рекламный модуль `wb_adv_*` и API/GAS-trigger сохранены в коде, но временно
не входят в ближайший рефакторинг.

---

## Стек

| Компонент        | Версия        |
|------------------|---------------|
| Python           | 3.11+         |
| PostgreSQL       | 16 (Docker)   |
| psycopg2-binary  | последняя     |
| requests         | последняя     |
| python-dotenv    | последняя     |
| Docker Desktop   | последняя     |

---

## Структура проекта

```
DataBase_MP/
├── app/
│   ├── admin/                      # Web-админка и backend-данные для UI
│   ├── clients/
│   │   ├── http_wb_statistics.py   # HTTP-клиент WB Statistics API
│   │   ├── http_wb_stocks.py       # HTTP-клиент WB Analytics API
│   │   ├── http_wb_order_feed.py   # Новый WB Analytics Order Feed API
│   │   ├── http_ozon_seller.py     # HTTP-клиент Ozon Seller API
│   │   └── http_api_erp_tru.py     # HTTP-клиент ERP/TRU API
│   ├── jobs/
│   │   ├── job_wb_orders.py        # WB заказы
│   │   ├── job_wb_order_feed.py    # WB лента заказов, текущие статусы
│   │   ├── job_wb_stocks.py        # WB остатки
│   │   ├── job_ozon_orders.py      # Ozon FBO заказы
│   │   ├── job_ozon_stocks.py      # Ozon остатки
│   │   ├── job_ozon_placement.py   # Ozon стоимость размещения в БД
│   │   ├── job_sheets_ozon_placement_export.py # Ozon хранение в Google Sheets
│   │   ├── job_api_erp_tru_product_stats.py # ERP/TRU статистика товаров
│   │   └── job_sheets_api_erp_tru_sales_export.py # ERP/TRU продажи в Google Sheets
│   ├── normalize/
│   │   ├── norm_wb_orders.py       # Нормализация WB
│   │   └── norm_ozon_orders.py     # Нормализация Ozon
│   ├── ops/                        # Штатные команды обслуживания проекта
│   ├── cli.py                      # Единый CLI управления
│   ├── api_server.py               # FastAPI server: триггер fullstats и админка
│   ├── db.py                       # Все функции работы с БД
│   └── utils.py                    # Общие утилиты (логирование, TG, время)
├── infra/
│   └── docker-compose.yml          # PostgreSQL 16 контейнер
├── scripts/
│   ├── run_wb_orders.cmd
│   ├── run_wb_stocks.cmd
│   ├── run_ozon_orders.cmd
│   ├── run_api_server.cmd
│   └── run_hidden.vbs              # Скрытый запуск scheduled jobs
├── .env                            # Секреты (не в git!)
├── .env.example                    # Шаблон переменных окружения
├── requirements.txt
└── README.md
```

---

## Быстрый старт

### 1. Настроить окружение

```powershell
copy DataBase_MP\.env.example DataBase_MP\.env
```

Открыть `.env` и заполнить:

```env
WB_TOKEN=ваш_токен_wb_api
PG_DSN=postgresql://app@localhost:5432/marketplace
POSTGRES_PASSWORD=ваш_пароль
TG_BOT_TOKEN=токен_бота         # опционально — алерты в Telegram
TG_CHAT_ID=id_чата              # опционально
```

**Где взять WB_TOKEN:** Личный кабинет WB → Настройки → Доступ к API.
Токен должен иметь доступ к категориям **Статистика** и **Аналитика**.

### 2. Поднять базу данных

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
docker compose -f infra\docker-compose.yml up -d
```

Проверить:

```powershell
docker ps
# должна быть строка: infra-db-1   postgres:16-alpine   Up
```

Если `.env` скопирован с сервера в уже существующий локальный Docker volume,
пароль пользователя `app` внутри контейнера может остаться старым. Тогда
синхронизировать локальный контейнер с `PG_DSN` из `.env`:

```powershell
python tools\sync_local_postgres_password.py
```

### 3. Установить зависимости Python

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 4. Бэкфилл — загрузить историю заказов (один раз)

```powershell
python app\jobs\job_wb_orders_backfill.py 2026-01-01
```

Загружает все заказы начиная с указанной даты постранично.
WB-лимит: 1 запрос в минуту → каждые 80 000 строк = +62 сек.
После обрыва можно запустить повторно — продолжит с последнего курсора.

### 5. Настроить автообновление (Планировщик задач Windows)

#### Заказы — каждый час

```powershell
$action  = New-ScheduledTaskAction -Execute "cmd.exe" `
           -Argument "/c `"C:\Програмирование\Проекты\DataBase_MP\scripts\run_wb_orders.cmd`""
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 60) -Once -At "00:00"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync" `
    -Action $action -Trigger $trigger -Settings $settings
```

#### Ozon FBO заказы — каждый час

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_ozon_orders_task.ps1
```

#### WB Лента заказов — каждые 15 минут

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_wb_order_feed_task.ps1
```

Задача `\DB_MP\WB_Order_Feed_Sync` повторно получает последние 31 сутки,
потому что WB меняет статус уже созданного заказа. Она сохраняет raw HTTP-ответы,
текущее техническое состояние и отдельную историю изменений по `srid`.
Старый `WB_Orders_Sync` пока продолжает работать: он нужен для истории до 31 дня
и для безопасного сравнения нового источника.

Задача в Планировщике будет называться
`\DB_MP\Ozon_Orders_Sync`. Она запускает
`scripts\run_ozon_orders.cmd` через скрытый wrapper `scripts\run_hidden.vbs`,
чтобы при плановом запуске не открывалось окно терминала. Job пишет текущие
строки заказов Ozon, raw-ответы API и историю изменений отправлений.

#### Остатки — каждые 30 минут

```powershell
$action  = New-ScheduledTaskAction -Execute "cmd.exe" `
           -Argument "/c `"C:\Програмирование\Проекты\DataBase_MP\scripts\run_wb_stocks.cmd`""
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 30) -Once -At "00:00"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Stocks_Sync" `
    -Action $action -Trigger $trigger -Settings $settings
```

---

## Как работает ETL

### Заказы (инкрементально)

```
Планировщик (каждый час)
  └── scripts\run_wb_orders.cmd
        └── app\jobs\job_wb_orders.py
              ├── Читает курсор из job_cursors
              ├── Вычисляет lookback (2–15 мин назад) для защиты от пропусков
              ├── GET WB Statistics API: lastChangeDate >= cursor - lookback
              ├── INSERT → wb_orders_raw_dedup  (дедупликация по srid + last_change_ts)
              ├── UPSERT → wb_orders_norm       (по srid, обновляет статусы отмены)
              ├── Обновляет курсор в job_cursors
              ├── Пишет метрики в job_runs
              └── Отправляет TG-алерт
```

**Lookback** — намеренный откат курсора на 2–15 минут. Защита от заказов,
у которых WB обновляет `lastChangeDate` с задержкой.

### Остатки (snapshot)

```
Планировщик (каждые 30 мин)
  └── scripts\run_wb_stocks.cmd
        └── app\jobs\job_wb_stocks.py
              ├── POST WB Analytics API (все склады, пагинация по offset)
              ├── INSERT → wb_stocks_raw   (полный слепок каждого запуска)
              ├── UPSERT → wb_stocks_snap  (актуальное состояние по ключу nm_id+chrt_id+warehouse_id)
              ├── Очистка wb_stocks_raw старше 30 дней
              ├── Пишет метрики в job_runs
              └── Отправляет TG-алерт
```

---

## Таблицы БД

| Таблица               | Назначение                                              |
|-----------------------|---------------------------------------------------------|
| `wb_orders_norm`      | Нормализованные заказы WB (основная, 27 полей)          |
| `raw.wb_order_feed_orders` | Текущее raw-состояние заказа из нового WB Order Feed |
| `raw.wb_order_feed_order_versions` | История изменений статусов нового WB Order Feed |
| `staging.wb_order_feed_orders_full` | Полная техническая таблица Order Feed |
| `wb_orders_raw_dedup` | Сырые JSON-версии изменений (хранятся 14 дней)          |
| `wb_stocks_snap`      | Актуальные остатки по складам (upsert по ключу)         |
| `wb_stocks_raw`       | Полный слепок каждого запроса остатков (30 дней)        |
| `job_cursors`         | Текущий курсор каждого джоба                            |
| `job_runs`            | История всех запусков с метриками                       |

---

## Мониторинг

Алерты Telegram показывают, что scheduled jobs живы. Для проверки целостности
проекта после изменений используйте локальные команды ниже.

### Проверка проекта после изменений

Единый способ управлять проектом:

```powershell
.\.venv\Scripts\python.exe -m app.cli health
.\.venv\Scripts\python.exe -m app.cli migrate --from-version 10 --to-version 14
.\.venv\Scripts\python.exe -m app.cli jobs-status --limit 10
.\.venv\Scripts\python.exe -m app.cli audit
.\.venv\Scripts\python.exe -m app.cli checks
.\.venv\Scripts\python.exe -m app.cli secrets status
```

Старые `tools\*.py` оставлены только как совместимые ярлыки или редкие
аудиторы. Постоянные операции запускаются через `python -m app.cli`.

Минимальная проверка после изменений:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
.\.venv\Scripts\python.exe -m compileall app tools tests
.\.venv\Scripts\python.exe -m app.cli audit
```

`audit` не запускает выгрузки WB/Ozon и не меняет БД. Он проверяет, что
репозиторий собран аккуратно: ключевые файлы на месте, зависимости читаются,
скрипты запуска выглядят ожидаемо, код компилируется, тесты проходят.

### Web-админка

Админка встроена в существующий FastAPI server.

```powershell
scripts\run_api_server.cmd
```

Открыть:

```text
http://localhost:8080/admin
```

В интерфейсе есть вкладки:

- **Админка** — база, секреты, последние jobs.
- **Лента заказов** — отдельно WB и Ozon.

Данные админки защищены `API_SERVER_TOKEN`. Вставьте токен в поле `API token`.
Значения секретов в интерфейсе не показываются, только статус.

### Аудит Google Таблицы `Аналитика МП`

Для переноса логики из Google Таблицы в PostgreSQL/Web используется read-only
аудит структуры, заголовков, формул и `IMPORTRANGE`:

```powershell
.\.venv\Scripts\python.exe tools\sheets_audit.py
```

По умолчанию читается таблица из
`GOOGLE_SHEETS_ANALYTICS_MP_SPREADSHEET_ID`, а service account берётся из
`GOOGLE_APPLICATION_CREDENTIALS`.

Результаты сохраняются локально в `local\audits\sheets\`. Эта папка
игнорируется git.

В WSL можно явно указать ключ из соседнего проекта:

```bash
.venv/bin/python tools/sheets_audit.py \
  --credentials /home/klimgaranin_job/projects/mp-gas/apps/analytics-mp/.secrets/google-service-account.json
```

Карта логики для переноса в PostgreSQL/Web ведётся в
`../Developer_Knowledge/projects/DataBase_MP/ANALYTICS_MP_LOGIC.md`.

### Экспорт заказов из PostgreSQL в Google Таблицу

Заказы WB и Ozon выгружаются из PostgreSQL в таблицу `Аналитика МП` на скрытый
лист `DATA` в одинаковом формате: `Дата`, `Артикул`, `Кол-во`, `Сумма`.
Отменённые заказы не выгружаются.

Окно данных по умолчанию: текущий месяц, полный прошлый месяц и полный
позапрошлый месяц. Например, 02.08.2026 выгружается период с 01.06.2026 по
02.08.2026.

Даты считаются так:

- Ozon — по дате из ЛК/Ozon CSV (`Принят в обработку`), то есть по UTC-дате API;
- WB — по UTC-дате API, чтобы совпадать с эталонной CSV-выгрузкой заказов.

Размещение блоков на листе:

- Ozon: `A:D`;
- колонка `E`: пустой разделитель;
- WB: `F:I`.

Важно: если лист `DATA` защищён, service account из
`GOOGLE_APPLICATION_CREDENTIALS` должен быть добавлен в редакторы защищённых
диапазонов `DATA!A:D` и `DATA!F:I`, иначе Google API вернёт ошибку
`protected cell`.

Обычный режим обновляет только новые и изменившиеся строки. Если в блоке
остались строки старее нужного 3-месячного окна, блок очищается и собирается
заново, чтобы лист не накапливал старые месяцы.

Перед записью job проверяет, сколько строк есть на листе `DATA`. Если строк
меньше, чем нужно для подготовленной выгрузки, Google Sheets API автоматически
добавляет недостающие строки и только потом записывает данные.

```powershell
.\.venv\Scripts\python.exe -m app.cli sheets wb-orders
.\.venv\Scripts\python.exe -m app.cli sheets ozon-orders
```

Штатная combined job с логом, `job_runs` и Telegram-алертом:

```powershell
scripts\run_sheets_orders_export.cmd
```

Регистрация плановой задачи Windows, после проверки прав service account на
защищённые диапазоны листа `DATA`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_sheets_orders_export_task.ps1
```

По умолчанию задача регистрируется каждый час на 12-й минуте часа. Это сделано,
чтобы сначала успели отработать hourly jobs обновления заказов в PostgreSQL, а
потом через короткую паузу обновилась Google Таблица.

Для разовой полной пересборки блока:

```powershell
.\.venv\Scripts\python.exe -m app.cli sheets wb-orders --mode replace
.\.venv\Scripts\python.exe -m app.cli sheets ozon-orders --mode replace
```

Для проверки без записи:

```powershell
.\.venv\Scripts\python.exe -m app.cli sheets ozon-orders --dry-run
```

Новая миграция `migrations/V10__marketplace_analytics_foundation.sql` создаёт
отдельные схемы `raw`, `staging`, `core`, `analytics` и первые таблицы для
будущего модуля аналитики. Она не меняет старые production-таблицы `public.wb_*`.

Если внешний `IMPORTRANGE`-источник сохранён локально как `.xlsx` или `.csv`,
его можно разобрать командой:

```powershell
.\.venv\Scripts\python.exe tools\source_file_audit.py local\source_exports\имя_файла.xlsx
```

`local\source_exports\` не попадает в git, туда можно класть рабочие выгрузки
для разбора структуры. Секреты в таких файлах лучше не хранить; если попадутся
похожие на секреты строки, audit-инструмент маскирует их в отчёте.

Для `.xlsm` с Power Query/VBA есть отдельный read-only аудит. Он не выполняет
макросы и не обновляет запросы:

```powershell
.\.venv\Scripts\python.exe tools\xlsm_powerquery_audit.py local\source_exports\Статистика.xlsm
```

### Джоб файловой статистики

`app\jobs\job_source_statistics.py` оформляет внутренние файловые источники в
той же форме, что WB jobs: отдельный entrypoint, нормализация, запись raw
snapshot, staging-таблицы, `job_runs`, лог и Telegram-алерт.

По умолчанию он читает только 1С-остатки и список заказов. WB уже грузится
действующими jobs, Ozon переводится на API, Яндекс в текущем этапе закрыт.

Файловые источники на Windows-сервере:

- список заказов: `\\tsclient\P\Список заказов`;
- остатки 1С: `\\tsclient\S\МП`.

Если Windows-пути доступны, job берёт самые свежие файлы напрямую:
список заказов из `.xlsx`, остатки 1С из `.txt` или `.xls`.
Если путь недоступен, остаётся fallback на материализованные таблицы
`Статистика.xlsm`.

Dry-run без записи в БД:

```powershell
set SOURCE_STATISTICS_DRY_RUN=1
.\.venv\Scripts\python.exe app\jobs\job_source_statistics.py
```

Боевой запуск после применения миграций:

```powershell
scripts\run_source_statistics.cmd
```

Новые таблицы:

- `raw.source_file_snapshots` — полный слепок прочитанных Excel tables.
- `staging.source_orders_daily` — переходная таблица для старых файловых
  заказов, по умолчанию не заполняется.
- `staging.source_stock_summary` — переходная таблица для старых файловых
  остатков, по умолчанию не заполняется.
- `staging.ozon_storage_costs` — старый файловый блок хранения Ozon, сейчас
  заменяется API job `ozon_placement`.
- `core.production_inventory_snapshot` — внутренние остатки из 1С-блока.
- `staging.supply_pipeline_current` — список заказов в производстве/пути.

### Ozon API jobs

Ozon переводится с файловых выгрузок на API по той же форме, что WB jobs:
клиент API, raw request/response logs, raw сущности, staging-таблицы и
`job_runs`.

Ozon orders:

```powershell
scripts\run_ozon_orders.cmd
```

`app\jobs\job_ozon_orders.py` читает `/v3/posting/fbo/list`. Первый запуск
берёт данные с 1 января текущего года, дальше job ведёт курсор в `job_cursors`
и подходит для почасовой актуализации. Raw postings сохраняются в
`raw.ozon_fbo_postings`, история изменений — в
`raw.ozon_fbo_posting_versions`, полные нормализованные товарные строки — в
`staging.ozon_fbo_order_items_full`, HTTP-ответы — в `raw.api_responses`.

Статус Ozon FBO обновляется по `posting_number`: при повторной выгрузке того же
отправления строка обновляется новым статусом и payload. Новая версия истории
создаётся только если полный payload отправления реально изменился.

Dry-run без API и БД:

```powershell
set OZON_ORDERS_DRY_RUN=1
.\.venv\Scripts\python.exe app\jobs\job_ozon_orders.py
```

Для Ozon нужны секреты `OZON_CLIENT_ID` и `OZON_API_KEY`. В production-режиме их
лучше хранить через `APP_SECRET_BACKEND=keyring`, а не в `.env`.

Ozon stocks:

```powershell
scripts\run_ozon_stocks.cmd
```

`app\jobs\job_ozon_stocks.py` берёт список товаров Ozon, детальную карточку и
остатки через Analytics Stocks. Raw product list/info/stocks сохраняются в
`raw.ozon_product_list_items`, `raw.ozon_product_info_items`,
`raw.ozon_analytics_stocks`; полные нормализованные строки остатков — в
`staging.ozon_stock_details`; текущая агрегированная витрина по кластерам — в
`staging.ozon_stock_by_cluster`.

Ozon placement/storage cost:

```powershell
scripts\run_ozon_placement.cmd
```

`app\jobs\job_ozon_placement.py` создаёт асинхронные отчёты
`/v1/report/placement/by-products/create` и
`/v1/report/placement/by-supplies/create`, ждёт готовности через
`/v1/report/info`, скачивает XLSX и сохраняет товарный raw-отчёт в
`raw.ozon_placement_reports`, исходный XLSX — в
`raw.ozon_placement_report_files`, полные строки с оригинальными колонками —
в `raw.ozon_placement_report_rows`. Технический разобранный слой хранится в
`staging.ozon_placement_by_products` и `staging.ozon_placement_cells`.
Отчёт по поставкам сохраняется отдельно в
`raw.ozon_placement_by_supplies_reports`,
`raw.ozon_placement_by_supplies_report_files`,
`raw.ozon_placement_by_supplies_report_rows` и
`staging.ozon_placement_by_supplies_cells`.

Скачанные XLSX дополнительно сохраняются локально в архиве:

```text
archive/reports/ozon-placement/YYYY-MM-DD/
```

Рядом пишется `checksums.sha256`. Это архив файлов для человека и проверки;
запись в БД остаётся основной и не меняется.

Бизнес-вид для таблицы строится отдельно в
`analytics.ozon_placement_latest_for_sheets`. Колонка
`Дней до первой платности` считается только из двух placement-отчётов:
`by-products` даёт количество экземпляров по SKU, `by-supplies` даёт общий
бесплатный объём и поставки со сроками окончания бесплатного периода. Поставки
сортируются по сроку, их объёмы последовательно вычитаются из бесплатного
объёма, и берётся первый срок, где бесплатного объёма становится меньше
количества экземпляров из `by-products`.

Диагностическая проверка соседнего отчёта Ozon “по поставкам”:

```powershell
.\.venv\Scripts\python.exe -m app.cli ozon placement-report by-supplies
```

Команда не пишет в БД и не меняет Google Таблицу. Она только скачивает XLSX в
`local\reports\ozon_placement\` и показывает оригинальные колонки отчёта.
Использовать вручную, потому что Ozon ограничивает placement-отчёты 5
созданиями в день.

Выгрузка Ozon платного хранения в Google Таблицу:

```powershell
.\.venv\Scripts\python.exe -m app.cli sheets ozon-placement
scripts\run_sheets_ozon_placement_export.cmd
```

Назначение: лист `DATA`, колонки `K:O`.
Формат: `Артикул`, `Платно, шт`, `Платно, л`, `Списано в день, RUB`,
`Дней до первой платности`.

Расписание:

- `ozon_orders` — каждый час;
- `ozon_placement` — один раз утром;
- `sheets_ozon_placement_export` — один раз утром через 2 минуты после
  `ozon_placement`;
- `ozon_placement_retry` — умная повторная попытка через пару часов: если
  сегодняшний непустой отчёт уже есть, ничего не делает; если Sheets утром
  использовал вчерашний/старый отчёт, повторно запускает `ozon_placement` и
  обновляет Sheets;
- `ozon_stocks` — два раза в день, в соответствии с лимитами/рекомендациями
  Ozon swagger.

По умолчанию `ozon_placement` запрашивает отчёт за текущую дату по Минску.
Например, 06.08.2026 в 07:00 будет запрошен период
`2026-08-06..2026-08-06`.

Если `sheets_ozon_placement_export` вынужден взять не сегодняшний отчёт
например потому, что утренний `by-products` был пустым, Telegram-алерт содержит
предупреждение. Retry-задачу можно зарегистрировать так:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_ozon_placement_retry_task.ps1
```

ERP/TRU продажи товаров:

```powershell
scripts\run_api_erp_tru_product_stats.cmd
scripts\run_sheets_api_erp_tru_sales_export.cmd
.\.venv\Scripts\python.exe -m app.cli sheets api-erp-tru-sales
```

`job_api_erp_tru_product_stats.py` читает
`/api/v1/product/stat_list/` за период от такого же числа прошлого месяца до
сегодня. Raw HTTP сохраняется в `raw.api_responses`, строки ответа — в
`raw.api_erp_tru_product_stat_rows`, текущий технический слой полностью
заменяется в `staging.api_erp_tru_product_stats_current`.

Для Google Sheets дублирующие артикулы группируются в
`analytics.api_erp_tru_sales_for_sheets`: `article -> SUM(sales_count)`.
Назначение: лист `DATA`, стартовая ячейка `AE1`, колонки `Артикул`, `Кол-во`.

Регистрация ежедневных задач:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_api_erp_tru_sales_tasks.ps1
```

Dry-run:

```powershell
set OZON_STOCKS_DRY_RUN=1
.\.venv\Scripts\python.exe app\jobs\job_ozon_stocks.py

set OZON_PLACEMENT_DRY_RUN=1
.\.venv\Scripts\python.exe app\jobs\job_ozon_placement.py
```

### Последние запуски

```sql
SELECT job_name, started_at, api_rows, norm_upserted, dup_pct, status, error
FROM job_runs
ORDER BY id DESC
LIMIT 20;
```

### Текущие курсоры

```sql
SELECT * FROM job_cursors;
```

### Статистика заказов по месяцам

```sql
SELECT
    DATE_TRUNC('month', date_ts) AS month,
    COUNT(*)                      AS orders,
    SUM(price_with_disc)          AS revenue,
    SUM(CASE WHEN is_cancel THEN 1 ELSE 0 END) AS cancels
FROM wb_orders_norm
GROUP BY 1
ORDER BY 1;
```

### Актуальные остатки (топ по количеству)

```sql
SELECT nm_id, SUM(quantity) AS total_qty
FROM wb_stocks_snap
GROUP BY nm_id
ORDER BY total_qty DESC
LIMIT 20;
```

---

## Управление планировщиком

```powershell
# Запустить вручную
Start-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync"
Start-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Stocks_Sync"

# Приостановить (например перед бэкфиллом)
Disable-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync"

# Возобновить
Enable-ScheduledTask -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync"

# Статус последнего запуска (0 = успех)
Get-ScheduledTaskInfo -TaskPath "\DB_MP\" -TaskName "WB_Orders_Sync" |
    Select LastRunTime, LastTaskResult, NextRunTime
```

---

## Переменные окружения и секреты

`.env` нужен для обычных настроек: режим, расписания, пути к логам, даты,
лимиты. Реальные токены и пароли в production-режиме лучше хранить в Windows
Credential Manager через `keyring`.

| Переменная                     | Обязательна | По умолчанию | Описание                               |
|--------------------------------|-------------|--------------|----------------------------------------|
| `APP_ENV`                      | ❌           | `local`      | Режим окружения                        |
| `APP_SECRET_BACKEND`           | ❌           | `env`        | Источник секретов: `env` или `keyring` |
| `APP_SECRET_SERVICE_NAME`      | ❌           | `DataBase_MP`| Имя сервиса для Windows Credential Manager |
| `GOOGLE_SHEETS_ANALYTICS_MP_SPREADSHEET_ID` | ❌ | ID таблицы `Аналитика МП` | Таблица-эталон для аудита |
| `GOOGLE_APPLICATION_CREDENTIALS` | ❌         | `secrets/google-service-account.json` | Путь к service account JSON |
| `SHEETS_ORDERS_EXPORT_LOG_FILE` | ❌          | `logs/job_sheets_orders_export.log` | Файл лога выгрузки заказов в Google Sheets |
| `SHEETS_ORDERS_EXPORT_MODE`     | ❌          | `upsert`     | Режим выгрузки заказов: `upsert` или `replace` |
| `SHEETS_ORDERS_EXPORT_DRY_RUN`  | ❌          | `0`          | Проверить Sheets job без записи        |
| `WB_TOKEN`                     | ✅           | —            | Секрет: общий токен WB API             |
| `WB_TOKEN_CONTENT`             | ❌           | `WB_TOKEN`   | Опциональный отдельный токен WB Advertising API |
| `PG_DSN`                       | ✅           | —            | Настройка подключения к PostgreSQL без пароля |
| `POSTGRES_PASSWORD`            | ✅           | —            | Секрет: пароль для Docker-контейнера   |
| `TG_BOT_TOKEN`                 | ❌           | —            | Секрет: токен Telegram-бота            |
| `TG_CHAT_ID`                   | ❌           | —            | Секрет: ID чата для алертов            |
| `LOG_LEVEL`                    | ❌           | `INFO`       | Уровень логов (DEBUG/INFO/WARNING)     |
| `WB_FIRST_RUN_DAYS_BACK`       | ❌           | `3`          | Глубина первого запуска заказов (дней) |
| `WB_LOOKBACK_MINUTES`          | ❌           | `10`         | Откат курсора заказов (минут)          |
| `WB_RAW_DEDUP_RETENTION_DAYS`  | ❌           | `14`         | Хранение сырых заказов (дней)          |
| `WB_STOCKS_RAW_RETENTION_DAYS` | ❌           | `30`         | Хранение сырых остатков (дней)         |
| `OZON_CLIENT_ID`               | ✅ для Ozon  | —            | Секрет: Client-Id Ozon Seller API     |
| `OZON_API_KEY`                 | ✅ для Ozon  | —            | Секрет: Api-Key Ozon Seller API       |
| `OZON_ORDERS_LOG_FILE`         | ❌           | —            | Файл лога Ozon orders job             |
| `OZON_ORDERS_DRY_RUN`          | ❌           | `0`          | Проверить период без API/БД           |
| `OZON_ORDERS_FIRST_RUN_DATE`   | ❌           | 1 января текущего года | Дата первого полного запуска |
| `OZON_ORDERS_LOOKBACK_MINUTES` | ❌           | `180`        | Откат курсора для защиты от задержек  |
| `OZON_ORDERS_SINCE`            | ❌           | —            | Ручное начало периода для разового запуска |
| `OZON_ORDERS_UNTIL`            | ❌           | —            | Ручной конец периода для разового запуска |
| `OZON_PLACEMENT_LOG_FILE`      | ❌           | —            | Файл лога Ozon placement job          |
| `OZON_PLACEMENT_DRY_RUN`       | ❌           | `0`          | Проверить placement без API/БД        |
| `OZON_PLACEMENT_DATE_FROM`     | ❌           | текущая дата Europe/Minsk | Начало периода отчёта placement |
| `OZON_PLACEMENT_DATE_TO`       | ❌           | текущая дата Europe/Minsk | Конец периода отчёта placement   |
| `OZON_PLACEMENT_POLL_ATTEMPTS` | ❌           | `20`         | Сколько раз ждать готовность отчёта   |
| `OZON_PLACEMENT_POLL_SLEEP_SECONDS` | ❌      | `30`         | Пауза между проверками отчёта         |
| `OZON_PLACEMENT_INCLUDE_SUPPLIES` | ❌        | `1`          | Загружать отчёт placement по поставкам |
| `SHEETS_OZON_PLACEMENT_EXPORT_LOG_FILE` | ❌  | `logs/job_sheets_ozon_placement_export.log` | Файл лога выгрузки Ozon хранения в Google Sheets |
| `SHEETS_OZON_PLACEMENT_EXPORT_MODE` | ❌     | `replace`    | Режим обновления блока `DATA!K:O`     |
| `SHEETS_OZON_PLACEMENT_EXPORT_DRY_RUN` | ❌  | `0`          | Проверить Sheets job без записи       |
| `API_ERP_TRU_TOKEN`             | ✅ для ERP/TRU | —         | Секрет: Bearer token ERP/TRU API      |
| `API_ERP_TRU_LOG_FILE`          | ❌           | `logs/job_api_erp_tru_product_stats.log` | Файл лога ERP/TRU product stats |
| `API_ERP_TRU_DRY_RUN`           | ❌           | `0`          | Проверить ERP/TRU job без API/БД      |
| `API_ERP_TRU_DATE_FROM`         | ❌           | такое же число прошлого месяца | Ручное начало периода ERP/TRU |
| `API_ERP_TRU_DATE_TO`           | ❌           | сегодня      | Ручной конец периода ERP/TRU          |
| `SHEETS_API_ERP_TRU_SALES_EXPORT_LOG_FILE` | ❌ | `logs/job_sheets_api_erp_tru_sales_export.log` | Файл лога выгрузки ERP/TRU продаж |
| `SHEETS_API_ERP_TRU_SALES_EXPORT_MODE` | ❌  | `replace`    | Режим обновления блока `DATA!AE:AF`   |
| `SHEETS_API_ERP_TRU_SALES_EXPORT_DRY_RUN` | ❌ | `0`        | Проверить Sheets job без записи       |
| `OZON_STOCKS_LOG_FILE`         | ❌           | —            | Файл лога Ozon stocks job             |
| `OZON_STOCKS_DRY_RUN`          | ❌           | `0`          | Проверить stocks без API/БД           |
| `SOURCE_STATISTICS_FILE`       | ❌           | `local/source_exports/Статистика.xlsm` | Файл источника статистики |
| `SOURCE_STATISTICS_LOG_FILE`   | ❌           | —            | Файл лога source statistics job        |
| `SOURCE_STATISTICS_DRY_RUN`    | ❌           | `0`          | Проверка без записи в БД               |
| `SOURCE_STATISTICS_INCLUDE_WB_TABLES` | ❌    | `0`          | Читать WB-блоки из Excel; обычно не нужно |
| `SOURCE_STATISTICS_ORDERS_LIST_PATH` | ❌     | `\\tsclient\P\Список заказов` | Папка/файл списка заказов на Windows-сервере |
| `SOURCE_STATISTICS_1C_STOCKS_PATH` | ❌        | `\\tsclient\S\МП` | Папка/файл остатков 1С на Windows-сервере |

### Секреты через Windows Credential Manager

Текущий рабочий режим поддерживает fallback на `.env`, чтобы не сломать
действующие Windows jobs. Production-режим должен хранить секреты через
`keyring`, то есть через Windows Credential Manager.

Для человека главный сейф — Bitwarden Desktop. В нём удобно хранить реальные
токены и пароли с понятными названиями. В рабочий запуск проекта секреты
переносятся из Bitwarden в Windows Credential Manager через команды ниже.

### Bitwarden -> Windows Credential Manager

Когда секрет создан или изменён в Bitwarden, его нужно подтянуть в рабочий
keyring. Команды вводит менеджер в Windows PowerShell, потому что Bitwarden
попросит мастер-пароль:

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
$env:BW_SESSION = $(bw unlock --raw)
.\.venv\Scripts\python.exe -m app.cli secrets pull-from-bitwarden
bw lock
```

Подтянуть один секрет:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets pull-from-bitwarden OZON_API_KEY
```

Команда берёт записи из папки Bitwarden `DataBase_MP` с именами вида
`DataBase_MP / SECRET_NAME`, обновляет одноимённые секреты в keyring и не
печатает значения.

### Windows Credential Manager -> Bitwarden

Если нужно перенести уже настроенные runtime-секреты из Windows Credential
Manager в Bitwarden, используется:

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
$env:BW_SESSION = $(bw unlock --raw)
.\.venv\Scripts\python.exe -m app.cli bitwarden push-from-keyring --dry-run
.\.venv\Scripts\python.exe -m app.cli bitwarden push-from-keyring
bw lock
```

### Первичный переход с `.env`

Безопасный переход:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m app.cli secrets migrate-from-env
.\.venv\Scripts\python.exe -m app.cli secrets status --backend keyring
```

После проверки в `.env` оставить только режим и несекретные настройки:

```env
APP_SECRET_BACKEND=keyring
APP_SECRET_SERVICE_NAME=DataBase_MP
PG_DSN=postgresql://app@localhost:5432/marketplace
```

`PG_DSN` должен быть без пароля. Пароль БД хранится отдельным секретом
`POSTGRES_PASSWORD`. Если старый `PG_DSN` уже содержит пароль, разделить его
можно командой:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets normalize-postgres --overwrite-password
```

Очистить `.env` после переноса секретов:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets clean-env --backend keyring
```

Ручная запись одного секрета:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets set WB_TOKEN
```

Команда попросит вставить значение два раза и не покажет его на экране.

Значения секретов в интерфейсах и проверках должны показываться только как
`задан` / `не задан`.

---

## Подключение в DBeaver

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `marketplace`
- **User:** `app`
- **Password:** значение секрета `POSTGRES_PASSWORD` из Windows Credential Manager
  или временно из `.env`, пока переход не завершён
