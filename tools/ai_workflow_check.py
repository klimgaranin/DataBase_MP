"""
Проверка AI workflow в соседнем Developer_Knowledge.

Инструмент не ходит в сеть и не читает секреты. Он проверяет, что базовые
инструкции, роли и workflow-файлы на месте. Если Developer_Knowledge не
склонирован рядом, проверка пропускается, чтобы чистый project repo оставался
самодостаточным для deploy.
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
KNOWLEDGE_ROOT = ROOT.parent / "Developer_Knowledge" / "projects" / ROOT.name

REQUIRED_FILES = [
    "AGENTS.md",
    "PROJECT_CONTEXT.md",
    "TASKS.md",
    "DECISIONS.md",
    "AI_WORKFLOW.md",
    "PROJECT_STANDARDS.md",
    "roles/mp_product_owner.md",
    "roles/mp_architect.md",
    "roles/etl_reviewer.md",
    "roles/db_safety.md",
    "roles/admin_ui_reviewer.md",
    "roles/release_manager.md",
    "roles/project_steward.md",
    "workflows/feature_intake.md",
    "workflows/change_review.md",
    "workflows/release_check.md",
    "workflows/cleanup_check.md",
    "templates/TASK_BRIEF.md",
]

REQUIRED_REFERENCES = {
    "AI_WORKFLOW.md": [
        "roles/mp_product_owner.md",
        "roles/mp_architect.md",
        "roles/etl_reviewer.md",
        "roles/db_safety.md",
        "roles/admin_ui_reviewer.md",
        "roles/release_manager.md",
        "roles/project_steward.md",
        "PROJECT_STANDARDS.md",
    ],
    "workflows/release_check.md": [
        "python tools/ai_workflow_check.py",
        "python tools/project_standards_check.py",
        "python -m unittest discover -s tests",
    ],
}


def main() -> int:
    if not KNOWLEDGE_ROOT.exists():
        print("SKIP: Developer_Knowledge рядом не найден")
        return 0

    errors: list[str] = []
    for rel_path in REQUIRED_FILES:
        path = KNOWLEDGE_ROOT / rel_path
        if not path.exists():
            errors.append(f"не найден файл: {rel_path}")
        elif not path.read_text(encoding="utf-8").strip():
            errors.append(f"пустой файл: {rel_path}")

    for rel_path, references in REQUIRED_REFERENCES.items():
        path = KNOWLEDGE_ROOT / rel_path
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
