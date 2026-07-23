from __future__ import annotations

from typing import Sequence


def run_project_audit() -> int:
    from tools.project_audit import main

    return main()


def run_ai_workflow_check() -> int:
    from tools.ai_workflow_check import main

    return main()


def run_project_standards_check() -> int:
    from tools.project_standards_check import main

    return main()


def run_checks(names: Sequence[str] | None = None) -> int:
    selected = list(names or ("ai-workflow", "project-standards", "audit"))
    commands = {
        "ai-workflow": run_ai_workflow_check,
        "project-standards": run_project_standards_check,
        "audit": run_project_audit,
    }

    failed = 0
    for name in selected:
        command = commands.get(name)
        if command is None:
            print(f"{name}: неизвестная проверка")
            failed += 1
            continue
        print(f"\n== {name} ==")
        code = command()
        if code != 0:
            failed += 1
    return 1 if failed else 0
