# DataBase_MP — Автоматизация загрузки данных маркетплейсов

Проект автоматически собирает данные с Wildberries в локальную PostgreSQL-базу через Docker.
Данные обновляются по расписанию через Планировщик задач Windows.

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

### 3. Установить зависимости Python

```powershell
pip install -r requirements.txt
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

---

## Подключение в DBeaver

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `marketplace`
- **User:** `app`
- **Password:** значение `POSTGRES_PASSWORD` из `.env`
