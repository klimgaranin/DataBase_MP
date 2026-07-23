# Управление секретами

## Простая схема

В проекте используются два уровня хранения:

- **Bitwarden Desktop** — главный сейф для человека. Здесь менеджер хранит
  реальные токены, пароли и инструкции к ним.
- **Windows Credential Manager** — техническое хранилище для запуска jobs.
  Python читает его через `keyring`.

Проектный `.env` не должен содержать реальные токены и пароли. В `.env` можно
оставлять только несекретные настройки: режим, адрес БД без пароля, пути к
файлам и флаги jobs.

## Что положить в Bitwarden

Создать папку или коллекцию:

```text
DataBase_MP
```

Рекомендуемые записи:

```text
DataBase_MP / WB_TOKEN
DataBase_MP / OZON_CLIENT_ID
DataBase_MP / OZON_API_KEY
DataBase_MP / POSTGRES_PASSWORD
DataBase_MP / TG_BOT_TOKEN
DataBase_MP / TG_CHAT_ID
DataBase_MP / API_SERVER_TOKEN
```

Для каждой записи:

- название — как в списке выше;
- username можно оставить пустым или указать `DataBase_MP`;
- password/value — само секретное значение;
- notes — простое описание: откуда токен, для чего нужен, когда обновлён.

WB-токен сейчас общий. Если отдельные `WB_TOKEN_CONTENT`,
`WB_ANALYTICS_TOKEN`, `WB_SUPPLIES_TOKEN` не заданы, проект использует
`WB_TOKEN`.

## Как перенести секрет из Bitwarden в рабочий запуск

1. Открыть Bitwarden Desktop.
2. Найти запись, например `DataBase_MP / OZON_API_KEY`.
3. Скопировать значение.
4. В Windows-папке проекта выполнить:

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
.\.venv\Scripts\python.exe -m app.cli secrets set OZON_API_KEY
```

5. Вставить значение два раза. Команда не показывает его на экране.
6. Проверить статус:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets status --backend keyring
```

Статус должен показывать только `задан` или `не задан`.

## PostgreSQL

`PG_DSN` хранится без пароля:

```env
PG_DSN=postgresql://app@localhost:5432/marketplace
```

Пароль хранится отдельно:

```text
POSTGRES_PASSWORD
```

Если старый `PG_DSN` содержит пароль, разделить его можно командой:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets normalize-postgres --overwrite-password
```

## Очистка `.env`

После переноса секретов:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets clean-env --backend keyring
```

В `.env` должны остаться только несекретные строки, например:

```env
APP_SECRET_BACKEND=keyring
APP_SECRET_SERVICE_NAME=DataBase_MP
PG_DSN=postgresql://app@localhost:5432/marketplace
```

## Что нельзя делать

- Не коммитить `.env`.
- Не вставлять токены в README, задачи, логи и ответы чата.
- Не хранить приватные ключи и service account JSON в git.
- Не отправлять скриншоты с открытыми токенами.
- Не переносить секреты из старых Excel/Power Query в код.

## Проверка

```powershell
.\.venv\Scripts\python.exe -m app.cli health --log-lines 0
.\.venv\Scripts\python.exe -m app.cli secrets status --backend keyring
```

Если обе команды проходят, jobs могут работать без секретов в `.env`.
