"""
Проверка проектных стандартов DataBase_MP.

Инструмент не читает секреты и не ходит в сеть. Он проверяет, что документы
стандартов на месте и что в корне нет очевидных случайных рабочих файлов.
"""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

REQUIRED_FILES = [
    ".codex/PROJECT_STANDARDS.md",
    ".codex/workflows/cleanup_check.md",
    ".codex/roles/project_steward.md",
]

REQUIRED_TEXT = {
    ".codex/PROJECT_STANDARDS.md": [
        "raw",
        "staging",
        "analytics",
        "TypedDict",
        "migrations/VNN__description.sql",
        "python tools/project_standards_check.py",
    ],
    ".codex/AI_WORKFLOW.md": [
        "roles/project_steward.md",
        "workflows/cleanup_check.md",
    ],
    "AGENTS.md": [
        ".codex/PROJECT_STANDARDS.md",
        "python tools/project_standards_check.py",
    ],
}

ALLOWED_ROOT_SUFFIXES = {
    ".md",
    ".txt",
    ".toml",
    ".yml",
    ".yaml",
    ".json",
    ".example",
}

ALLOWED_ROOT_FILES = {
    ".env",
    ".env.example",
    ".gitignore",
    "AGENTS.md",
    "README.md",
    "requirements.txt",
}

ALLOWED_ROOT_DIRS = {
    ".agents",
    ".codex",
    ".git",
    ".venv",
    ".vscode",
    "app",
    "archive",
    "evidence",
    "infra",
    "local",
    "migrations",
    "scripts",
    "tests",
    "tools",
}


def main() -> int:
    errors: list[str] = []

    for rel_path in REQUIRED_FILES:
        path = ROOT / rel_path
        if not path.exists():
            errors.append(f"не найден файл стандарта: {rel_path}")
        elif not path.read_text(encoding="utf-8").strip():
            errors.append(f"пустой файл стандарта: {rel_path}")

    for rel_path, required_parts in REQUIRED_TEXT.items():
        path = ROOT / rel_path
        if not path.exists():
            errors.append(f"не найден файл для проверки текста: {rel_path}")
            continue
        text = path.read_text(encoding="utf-8")
        for part in required_parts:
            if part not in text:
                errors.append(f"{rel_path}: нет обязательного упоминания {part}")

    for item in ROOT.iterdir():
        name = item.name
        if item.is_dir():
            if name not in ALLOWED_ROOT_DIRS:
                errors.append(f"подозрительная папка в корне: {name}")
            continue
        if name in ALLOWED_ROOT_FILES:
            continue
        if item.suffix.lower() not in ALLOWED_ROOT_SUFFIXES:
            errors.append(f"подозрительный файл в корне: {name}")

    gitignore = ROOT / ".gitignore"
    if gitignore.exists():
        ignored = {
            line.strip()
            for line in gitignore.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        if ".env" not in ignored:
            errors.append(".gitignore: .env должен быть явно проигнорирован")
    else:
        errors.append("не найден .gitignore")

    if errors:
        print("FAIL: стандарты проекта требуют правки")
        for error in errors:
            print(f"- {error}")
        return 1

    print("OK: стандарты проекта на месте")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
