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
│   ├── clients/
│   │   ├── http_wb_statistics.py   # HTTP-клиент WB Statistics API (заказы)
│   │   └── http_wb_stocks.py       # HTTP-клиент WB Analytics API (остатки)
│   ├── jobs/
│   │   ├── job_wb_orders.py        # Инкрементальный джоб заказов
│   │   ├── job_wb_orders_backfill.py # Разовый бэкфилл исторических заказов
│   │   └── job_wb_stocks.py        # Джоб остатков (каждые 30 мин)
│   ├── normalize/
│   │   ├── norm_wb_orders.py       # Нормализация заказов
│   │   └── norm_wb_stocks.py       # Нормализация остатков
│   ├── db.py                       # Все функции работы с БД
│   └── utils.py                    # Общие утилиты (логирование, TG, время)
├── infra/
│   └── docker-compose.yml          # PostgreSQL 16 контейнер
├── scripts/
│   ├── run_wb_orders.cmd           # Точка входа для планировщика (заказы)
│   └── run_wb_stocks.cmd           # Точка входа для планировщика (остатки)
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
PG_DSN=postgresql://app:ваш_пароль@localhost:5432/marketplace
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
Register-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync" `
    -Action $action -Trigger $trigger -Settings $settings
```

#### Ozon FBO заказы — каждый час

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_ozon_orders_task.ps1
```

Задача в Планировщике будет называться
`\MarketplaceDB\Ozon_Orders_Sync`. Она запускает
`scripts\run_ozon_orders.cmd`, который пишет текущие строки заказов Ozon,
raw-ответы API и историю изменений отправлений.

#### Остатки — каждые 30 минут

```powershell
$action  = New-ScheduledTaskAction -Execute "cmd.exe" `
           -Argument "/c `"C:\Програмирование\Проекты\DataBase_MP\scripts\run_wb_stocks.cmd`""
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 30) -Once -At "00:00"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Stocks_Sync" `
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

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
.\.venv\Scripts\python.exe -m compileall app tools tests
.\.venv\Scripts\python.exe tools\project_audit.py
```

`tools\project_audit.py` не запускает выгрузки WB и не меняет БД. Он проверяет,
что репозиторий собран аккуратно: ключевые файлы на месте, зависимости читаются,
скрипты запуска выглядят ожидаемо, код компилируется, тесты проходят.

Для диагностического просмотра окружения и БД без запуска WB API:

```powershell
.\.venv\Scripts\python.exe tools\health_check.py
```

Применить SQL-миграции:

```powershell
.\.venv\Scripts\python.exe tools\apply_migrations.py --from-version 10 --to-version 14
```

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
`.codex/ANALYTICS_MP_LOGIC.md`.

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

Для Ozon нужны секреты `OZON_CLIENT_ID` и `OZON_API_KEY`. В целевом режиме их
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

`app\jobs\job_ozon_placement.py` создаёт асинхронный отчёт
`/v1/report/placement/by-products/create`, ждёт готовности через
`/v1/report/info`, скачивает XLSX и сохраняет raw-отчёт в
`raw.ozon_placement_reports`, строки отчёта — в
`staging.ozon_placement_by_products`.

Расписание:

- `ozon_orders` — каждый час;
- `ozon_placement` — один раз утром;
- `ozon_stocks` — два раза в день, в соответствии с лимитами/рекомендациями
  Ozon swagger.

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
Start-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"
Start-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Stocks_Sync"

# Приостановить (например перед бэкфиллом)
Disable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# Возобновить
Enable-ScheduledTask -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync"

# Статус последнего запуска (0 = успех)
Get-ScheduledTaskInfo -TaskPath "\MarketplaceDB\" -TaskName "WB_Orders_Sync" |
    Select LastRunTime, LastTaskResult, NextRunTime
```

---

## Переменные окружения (.env)

| Переменная                     | Обязательна | По умолчанию | Описание                               |
|--------------------------------|-------------|--------------|----------------------------------------|
| `APP_ENV`                      | ❌           | `local`      | Режим окружения                        |
| `APP_SECRET_BACKEND`           | ❌           | `env`        | Источник секретов: `env` или `keyring` |
| `APP_SECRET_SERVICE_NAME`      | ❌           | `DataBase_MP`| Имя сервиса для Windows Credential Manager |
| `GOOGLE_SHEETS_ANALYTICS_MP_SPREADSHEET_ID` | ❌ | ID таблицы `Аналитика МП` | Таблица-эталон для аудита |
| `GOOGLE_APPLICATION_CREDENTIALS` | ❌         | `secrets/google-service-account.json` | Путь к service account JSON |
| `WB_TOKEN`                     | ✅           | —            | Токен WB API (Статистика + Аналитика)  |
| `PG_DSN`                       | ✅           | —            | DSN подключения к PostgreSQL           |
| `POSTGRES_PASSWORD`            | ✅           | —            | Пароль для Docker-контейнера           |
| `TG_BOT_TOKEN`                 | ❌           | —            | Токен Telegram-бота                    |
| `TG_CHAT_ID`                   | ❌           | —            | ID чата для алертов                    |
| `LOG_LEVEL`                    | ❌           | `INFO`       | Уровень логов (DEBUG/INFO/WARNING)     |
| `WB_FIRST_RUN_DAYS_BACK`       | ❌           | `3`          | Глубина первого запуска заказов (дней) |
| `WB_LOOKBACK_MINUTES`          | ❌           | `10`         | Откат курсора заказов (минут)          |
| `WB_RAW_DEDUP_RETENTION_DAYS`  | ❌           | `14`         | Хранение сырых заказов (дней)          |
| `WB_STOCKS_RAW_RETENTION_DAYS` | ❌           | `30`         | Хранение сырых остатков (дней)         |
| `OZON_CLIENT_ID`               | ✅ для Ozon  | —            | Client-Id Ozon Seller API             |
| `OZON_API_KEY`                 | ✅ для Ozon  | —            | Api-Key Ozon Seller API               |
| `OZON_ORDERS_LOG_FILE`         | ❌           | —            | Файл лога Ozon orders job             |
| `OZON_ORDERS_DRY_RUN`          | ❌           | `0`          | Проверить период без API/БД           |
| `OZON_ORDERS_FIRST_RUN_DATE`   | ❌           | 1 января текущего года | Дата первого полного запуска |
| `OZON_ORDERS_LOOKBACK_MINUTES` | ❌           | `180`        | Откат курсора для защиты от задержек  |
| `OZON_ORDERS_SINCE`            | ❌           | —            | Ручное начало периода для разового запуска |
| `OZON_ORDERS_UNTIL`            | ❌           | —            | Ручной конец периода для разового запуска |
| `OZON_PLACEMENT_LOG_FILE`      | ❌           | —            | Файл лога Ozon placement job          |
| `OZON_PLACEMENT_DRY_RUN`       | ❌           | `0`          | Проверить placement без API/БД        |
| `OZON_PLACEMENT_DATE_FROM`     | ❌           | вчера        | Начало периода отчёта placement       |
| `OZON_PLACEMENT_DATE_TO`       | ❌           | вчера        | Конец периода отчёта placement        |
| `OZON_PLACEMENT_POLL_ATTEMPTS` | ❌           | `20`         | Сколько раз ждать готовность отчёта   |
| `OZON_PLACEMENT_POLL_SLEEP_SECONDS` | ❌      | `30`         | Пауза между проверками отчёта         |
| `OZON_STOCKS_LOG_FILE`         | ❌           | —            | Файл лога Ozon stocks job             |
| `OZON_STOCKS_DRY_RUN`          | ❌           | `0`          | Проверить stocks без API/БД           |
| `SOURCE_STATISTICS_FILE`       | ❌           | `local/source_exports/Статистика.xlsm` | Файл источника статистики |
| `SOURCE_STATISTICS_LOG_FILE`   | ❌           | —            | Файл лога source statistics job        |
| `SOURCE_STATISTICS_DRY_RUN`    | ❌           | `0`          | Проверка без записи в БД               |
| `SOURCE_STATISTICS_INCLUDE_WB_TABLES` | ❌    | `0`          | Читать WB-блоки из Excel; обычно не нужно |
| `SOURCE_STATISTICS_ORDERS_LIST_PATH` | ❌     | `\\tsclient\P\Список заказов` | Папка/файл списка заказов на Windows-сервере |
| `SOURCE_STATISTICS_1C_STOCKS_PATH` | ❌        | `\\tsclient\S\МП` | Папка/файл остатков 1С на Windows-сервере |

### Секреты взрослее `.env`

Текущий рабочий режим пока поддерживает `.env`, чтобы не сломать действующие
Windows jobs. Новый слой `app/secrets.py` уже умеет читать секреты через
`keyring`, то есть через Windows Credential Manager.

Для перехода:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -c "import keyring; keyring.set_password('DataBase_MP', 'WB_TOKEN', 'вставьте_токен')"
```

В `.env` после этого можно поставить:

```env
APP_SECRET_BACKEND=keyring
APP_SECRET_SERVICE_NAME=DataBase_MP
```

Значения секретов в интерфейсах и проверках должны показываться только как
`задан` / `не задан`.

---

## Подключение в DBeaver

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `marketplace`
- **User:** `app`
- **Password:** значение `POSTGRES_PASSWORD` из `.env`
