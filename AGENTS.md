# AGENTS.md

## Роль проекта

`DataBase_MP` — рабочий Python/PostgreSQL проект для автоматизации данных
Wildberries: заказы, остатки, рекламные кампании и fullstats.

Проект используется из Windows. Копия в WSL может быть свежим клоном из git,
но боевые сетевые проверки WB иногда нужно запускать через Windows-мост.

## Правила работы

- Работать на русском языке.
- Перед изменениями читать `.codex/PROJECT_CONTEXT.md`, `.codex/TASKS.md`
  и `.codex/DECISIONS.md`.
- Для нетривиальных задач применять локальный AI workflow:
  - общий процесс: `.codex/AI_WORKFLOW.md`;
  - стандарты проекта: `.codex/PROJECT_STANDARDS.md`;
  - роли: `.codex/roles/`;
  - чеклисты: `.codex/workflows/`.
- Не ломать действующие джобы:
  - `app/jobs/job_wb_orders.py`
  - `app/jobs/job_wb_stocks.py`
  - `app/jobs/job_wb_adv_campaigns.py`
  - `app/jobs/job_wb_adv_fullstats.py`
  - `app/api_server.py`
- Не менять `.env` без прямого запроса. Секреты не выводить в ответы.
- Для новых переменных окружения обновлять `.env.example`, но не заполнять
  реальные значения.
- Учитывать Windows-окружение: `.cmd` scripts и Task Scheduler являются
  основным способом запуска production jobs.
- Для боевых WB-запросов из WSL сначала проверить сеть. Если прямой WSL-доступ
  не работает, использовать Windows-мост:

```powershell
powershell.exe -NoProfile -Command 'Push-Location "\\wsl.localhost\Ubuntu\home\klimgaranin_job\projects\DataBase_MP"; python <script>; exit $LASTEXITCODE'
```

- Через Windows-мост не запускать сложный `python -c` с вложенными кавычками,
  JSON, dict/list literal или многострочным кодом. Для таких проверок создавать
  временный/служебный `.py` файл или использовать существующий tool/script.
  `python -c` допустим только для простых однострочных команд без сложных
  кавычек.

## Архивы

Разовая юридическая WB-выгрузка от 2026-07-18 вынесена в:

`archive/wb-legal-export-20260718/`

Это архив, не production-код. Не удалять без явного запроса.

## Проверки

Минимум после изменений:

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

Для Windows-совместимости при изменении запускаемых джобов дополнительно
проверять через Windows Python.
