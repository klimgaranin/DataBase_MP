# Workflow: Release Check

Использовать перед пушем, деплоем, боевым запуском или передачей пользователю
готового результата.

## Чеклист

- `git status --short` просмотрен.
- Секреты не выводились и не попали в индекс.
- Миграции применены или явно не требуются.
- WSL-проверки прошли.
- Windows-мост проверен для jobs/scripts.
- База проверена read-only SQL-запросами, если менялись данные.
- Пользователю понятно, где смотреть результат.

## Команды

```bash
python -m compileall app tools tests
python -m unittest discover -s tests
python tools/project_audit.py
python tools/ai_workflow_check.py
python tools/project_standards_check.py
```

Windows-мост:

```powershell
powershell.exe -NoProfile -Command 'Push-Location "\\wsl.localhost\Ubuntu\home\klimgaranin_job\projects\DataBase_MP"; python -m unittest discover -s tests; exit $LASTEXITCODE'
```
