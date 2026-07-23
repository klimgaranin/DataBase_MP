from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.ops.migrations import (
    apply_migrations,
    main,
    migration_version,
    selected_migrations,
)

_version = migration_version


if __name__ == "__main__":
    raise SystemExit(main())
