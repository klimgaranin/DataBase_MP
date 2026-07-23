from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.ops.health import (
    check_db,
    dependency_status,
    load_health_env,
    main,
    mask_secret,
    safe_dsn_summary,
)

_safe_dsn_summary = safe_dsn_summary
_load_env = load_health_env
_dependency_status = dependency_status
_check_db = check_db


if __name__ == "__main__":
    raise SystemExit(main())
