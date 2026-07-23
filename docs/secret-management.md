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

## Как подтянуть секреты из Bitwarden в рабочий запуск

Когда секрет создан или изменён в Bitwarden, его нужно подтянуть в Windows
Credential Manager. Команды вводит менеджер в Windows PowerShell, потому что
Bitwarden попросит мастер-пароль:

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
$env:BW_SESSION = $(bw unlock --raw)
.\.venv\Scripts\python.exe -m app.cli secrets pull-from-bitwarden
bw lock
```

Команда берёт записи из папки `DataBase_MP` с именами вида
`DataBase_MP / SECRET_NAME` и обновляет одноимённые секреты в keyring.

Подтянуть один секрет:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets pull-from-bitwarden OZON_API_KEY
```

Не перезаписывать уже заданные значения:

```powershell
.\.venv\Scripts\python.exe -m app.cli secrets pull-from-bitwarden --no-overwrite
```

Bitwarden CLI должен быть разблокирован:

```powershell
$env:BW_SESSION = $(bw unlock --raw)
```

Команда не печатает значения секретов.

## Как вручную перенести секрет из Bitwarden в рабочий запуск

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

## Как перенести текущие рабочие секреты в Bitwarden

На Windows установлен Bitwarden CLI `bw`. Desktop-вход и CLI-вход разные. Если
CLI ещё не залогинен, сначала выполнить:

```powershell
bw login
```

Перед переносом разблокировать CLI в текущем окне PowerShell:

```powershell
$env:BW_SESSION = $(bw unlock --raw)
```

После этого проектный скрипт создаст папку `DataBase_MP` и записи по секретам из
Windows Credential Manager:

```powershell
cd C:\Програмирование\Проекты\DataBase_MP
.\.venv\Scripts\python.exe tools\sync_bitwarden_from_keyring.py --dry-run
.\.venv\Scripts\python.exe tools\sync_bitwarden_from_keyring.py
bw lock
```

Скрипт не печатает значения секретов. Он создаёт/обновляет записи вида:

```text
DataBase_MP / WB_TOKEN
DataBase_MP / OZON_CLIENT_ID
DataBase_MP / OZON_API_KEY
DataBase_MP / POSTGRES_PASSWORD
```

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
