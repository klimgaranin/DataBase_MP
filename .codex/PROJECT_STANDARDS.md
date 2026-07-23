# PROJECT_STANDARDS

## Цель

Этот документ задаёт порядок разработки `DataBase_MP`: куда класть файлы, как
называть таблицы, как типизировать данные и как убирать временное после задач.

Главное правило: проект должен быть понятен человеку, который открыл его через
месяц. Новая сущность должна иметь своё место, понятное имя, проверку и запись в
контексте проекта.

## Слои проекта

### Корень проекта

В корне должны быть только управляющие файлы:

- `AGENTS.md` — правила для Codex.
- `README.md` — инструкция для человека.
- `requirements.txt` — Python-зависимости.
- `.env.example` — пример настроек без секретов.
- `.gitignore` — защита от мусора и секретов.
- `app/` — production Python-код.
- `migrations/` — SQL-миграции.
- `scripts/` — Windows `.cmd` entrypoints.
- `tools/` — служебные проверки, аудит, миграции.
- `tests/` — unit/smoke тесты.
- `infra/` — Docker/инфраструктура.
- `.codex/` — проектная память, стандарты, workflow.
- `archive/` — архивы завершённых разовых задач.
- `local/` — локальные входные файлы, аудиты, временные результаты разработки.

Не класть в корень новые `.py`, `.xlsx`, `.json`, `.txt`, evidence, отчёты и
разовые выгрузки. Для них есть `tools/`, `local/`, `archive/`, `evidence/` или
отдельная папка задачи.

### `app/`

Текущая структура:

- `app/clients/` — HTTP/file клиенты. Только получение данных и transport.
- `app/jobs/` — запускаемые entrypoints jobs.
- `app/normalize/` — преобразование raw API/file строк в технические строки.
- `app/integrations/` — каталог API-методов и metadata.
- `app/config.py` — конфигурация без секретов в коде.
- `app/secrets.py` — доступ к секретам без вывода значений.
- `app/db.py` — текущий DB-слой. Исторически большой файл, постепенно делить.

Целевая структура для новых крупных блоков:

- `app/schemas/` — типы строк и контрактов между слоями.
- `app/repositories/` — запись/чтение БД.
- `app/services/` — бизнес-процессы поверх клиентов и БД.
- `app/api/` — будущий backend API.
- `app/admin/` — будущая админка/backend endpoints, если понадобится.

Новые папки добавлять только когда появляется реальный код, не создавать пустую
архитектуру ради архитектуры.

## Именование Python-файлов

Клиенты:

- `app/clients/http_wb_<domain>.py`
- `app/clients/http_ozon_<domain>.py`
- `app/clients/local_<source>.py`

Jobs:

- `app/jobs/job_wb_<domain>.py`
- `app/jobs/job_ozon_<domain>.py`
- `app/jobs/job_source_<domain>.py`

Нормализация:

- `app/normalize/norm_wb_<domain>.py`
- `app/normalize/norm_ozon_<domain>.py`
- `app/normalize/norm_source_<domain>.py`

Тесты:

- `tests/test_<domain>.py`
- Для job logic: тестировать чистые функции отдельно от реального API/БД.

Tools:

- `tools/<action>_<target>.py`, если инструмент повторяемый.
- Одноразовый временный script не оставлять в `tools/`; после задачи удалить
  или перенести в `archive/`, если он нужен как evidence.

## Типизация Python

Новые Python-файлы:

- начинать с `from __future__ import annotations`;
- типизировать входы и выходы всех новых функций;
- использовать `dict[str, Any]` только на границе raw API/file ответа;
- после нормализации использовать явные типы: `TypedDict`, `dataclass` или
  хорошо документированный `dict` с тестом;
- не протаскивать сырой `payload` как рабочую структуру через весь код;
- parse-функции должны быть явными: `parse_int`, `parse_float`, `parse_dt`,
  `parse_bool`;
- nullable поля обозначать как `T | None`;
- даты внутри Python держать как timezone-aware `datetime`, если это момент
  времени;
- публичные DB/job/client функции должны иметь понятное имя действия:
  `fetch_*`, `iter_*`, `normalize_*`, `insert_*`, `upsert_*`, `replace_*`.

Когда данные проходят между `normalize` и `db` в нескольких местах, для них
нужно заводить тип в `app/schemas/`. Если строка используется только внутри
одного job и покрыта тестом, допустим локальный `dict[str, Any]`.

## Стандарт БД

### Схемы

- `public` — старые production WB tables. Не расширять без необходимости.
- `raw` — сырой API/file слой и HTTP logs.
- `staging` — полный технический нормализованный слой.
- `core` — общие справочники и устойчивые сущности.
- `analytics` — views/витрины для DBeaver, backend и frontend.

### Raw

Raw хранит исходный ответ и страховку:

- полный `payload`;
- внешний ID;
- `source_run_id`;
- время загрузки;
- минимальные индексы для пересборки staging.

Raw не является рабочей таблицей для бизнес-расчётов.

### Staging

Staging должен быть полным техническим слоем:

- API/file поля раскрыты в колонки;
- вложенные массивы развернуты в строки или отдельные технические таблицы;
- ключи соответствуют реальной сущности;
- есть `source_run_id`;
- есть `updated_at` или `loaded_at`;
- `payload` можно оставить как страховочный след, но нельзя хранить полезные
  поля только там.

Для новых API-блоков правило: одна полная техническая таблица лучше трёх
укороченных переходных. Например:

- `raw.ozon_fbo_postings`
- `staging.ozon_fbo_order_items_full`
- `analytics.ozon_fbo_order_items_flat`

### Analytics

Analytics — это удобные обёртки:

- короткие имена колонок для работы;
- текущие снимки;
- агрегаты;
- отчётные views.

Analytics не должен быть единственным местом, где существует важное поле.

### Именование таблиц

Raw:

- `raw.<marketplace>_<domain>_<entity>`
- `raw.api_responses`
- `raw.source_file_snapshots`

Staging:

- `staging.<marketplace>_<domain>_<entity>_full` для полного технического слоя;
- `staging.<marketplace>_<domain>_<entity>_details` для исторических detail;
- `staging.<marketplace>_<domain>_<entity>_current` для текущего состояния;
- `staging.<source>_<entity>` для файловых источников.

Analytics:

- `analytics.<marketplace>_<domain>_<entity>_flat`;
- `analytics.<marketplace>_<domain>_<entity>_latest_flat`;
- `analytics.<business_area>_<metric>`.

### Миграции

- Все изменения БД только через `migrations/VNN__description.sql`.
- Номер миграции не переиспользовать.
- Backfill сначала делать из raw, а не новым API-запросом.
- Старые таблицы удалять только если они новые и не используются production
  кодом, либо после отдельного решения.
- Перед `DROP TABLE` убедиться, что новая таблица заполнена и view пересоздана.
- Для больших операций контролировать время и после миграции проверять counts.

## Jobs

Каждый job должен иметь:

- entrypoint в `app/jobs/job_<name>.py`;
- Windows script в `scripts/run_<name>.cmd`;
- advisory lock;
- запись в `job_runs`;
- русские логи по этапам;
- Telegram-итог;
- raw request/response logs для API;
- нормализацию отдельно от HTTP/file клиента;
- тестируемые helper-функции без реального API.

## Временные файлы и уборка

Разрешённые места:

- `/tmp` — временное на время проверки.
- `local/audits/` — локальные аудиты, которые не идут в git.
- `local/source_exports/` — входные файлы пользователя.
- `archive/` — завершённые разовые задачи, которые нужно сохранить.
- `evidence/` — доказательные выгрузки, если задача явно такая.

После задачи:

- удалить временные `.py`, `.json`, `.xlsx`, `.txt`, если они больше не нужны;
- если нужно сохранить — перенести в `archive/` или `local/` и отметить в
  `.codex/TASKS.md`;
- не оставлять служебные файлы в корне;
- проверить `git status --short`.

## Документирование

Обновлять `.codex/PROJECT_CONTEXT.md`, если появились:

- новая папка;
- новый job;
- новая таблица/view;
- новый способ запуска;
- новый важный tool.

Обновлять `.codex/DECISIONS.md`, если появилось правило или архитектурное
решение.

Обновлять `.codex/TASKS.md`, если задача завершена, появился следующий шаг или
изменился статус.

## Минимальные проверки

Перед финальным ответом после изменений:

```bash
python -m compileall app tools tests
python -m unittest discover -s tests
python tools/project_audit.py
python tools/ai_workflow_check.py
```

Если менялись jobs/scripts:

```powershell
powershell.exe -NoProfile -Command 'Push-Location "\\wsl.localhost\Ubuntu\home\klimgaranin_job\projects\DataBase_MP"; python -m unittest discover -s tests; exit $LASTEXITCODE'
```

Если менялись стандарты:

```bash
python tools/project_standards_check.py
```

