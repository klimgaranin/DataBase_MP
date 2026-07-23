"""
Проверка локального AI workflow в .codex.

Инструмент не ходит в сеть и не читает секреты. Он проверяет, что базовые
инструкции, роли и workflow-файлы на месте.
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

REQUIRED_FILES = [
    "AGENTS.md",
    ".codex/PROJECT_CONTEXT.md",
    ".codex/TASKS.md",
    ".codex/DECISIONS.md",
    ".codex/AI_WORKFLOW.md",
    ".codex/PROJECT_STANDARDS.md",
    ".codex/roles/mp_product_owner.md",
    ".codex/roles/mp_architect.md",
    ".codex/roles/etl_reviewer.md",
    ".codex/roles/db_safety.md",
    ".codex/roles/admin_ui_reviewer.md",
    ".codex/roles/release_manager.md",
    ".codex/roles/project_steward.md",
    ".codex/workflows/feature_intake.md",
    ".codex/workflows/change_review.md",
    ".codex/workflows/release_check.md",
    ".codex/workflows/cleanup_check.md",
    ".codex/templates/TASK_BRIEF.md",
]

REQUIRED_REFERENCES = {
    ".codex/AI_WORKFLOW.md": [
        "roles/mp_product_owner.md",
        "roles/mp_architect.md",
        "roles/etl_reviewer.md",
        "roles/db_safety.md",
        "roles/admin_ui_reviewer.md",
        "roles/release_manager.md",
        "roles/project_steward.md",
        ".codex/PROJECT_STANDARDS.md",
    ],
    ".codex/workflows/release_check.md": [
        "python tools/ai_workflow_check.py",
        "python tools/project_standards_check.py",
        "python -m unittest discover -s tests",
    ],
}


def main() -> int:
    errors: list[str] = []
    for rel_path in REQUIRED_FILES:
        path = ROOT / rel_path
        if not path.exists():
            errors.append(f"не найден файл: {rel_path}")
        elif not path.read_text(encoding="utf-8").strip():
            errors.append(f"пустой файл: {rel_path}")

    for rel_path, references in REQUIRED_REFERENCES.items():
        path = ROOT / rel_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for reference in references:
            if reference not in text:
                errors.append(f"{rel_path}: нет ссылки/упоминания {reference}")

    if errors:
        print("FAIL: AI workflow требует правки")
        for error in errors:
            print(f"- {error}")
        return 1

    print("OK: AI workflow проекта на месте")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
