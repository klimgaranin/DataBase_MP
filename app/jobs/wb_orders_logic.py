from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.utils import ensure_tz, parse_iso_dt


def calc_lookback(cursor_old_iso: str, last_dup_pct: float | None, base: int, max_look: int) -> int:
    dt_old = parse_iso_dt(cursor_old_iso)
    if dt_old is None:
        return base
    gap_min = max(int((datetime.now(dt_old.tzinfo or timezone.utc) - dt_old).total_seconds() // 60), 0)

    if gap_min <= 20:
        look = min(base, 2)
    elif gap_min <= 60:
        look = min(base, 5)
    elif gap_min <= 360:
        look = min(base, 10)
    else:
        look = min(max_look, max(base, 15))

    if last_dup_pct is not None:
        if last_dup_pct >= 30.0:
            look = min(look, 2)
        elif last_dup_pct >= 15.0:
            look = min(look, 5)
        elif last_dup_pct <= 3.0:
            look = min(max_look, look + 5)
        elif last_dup_pct <= 7.0:
            look = min(max_look, look + 2)

    return max(0, int(look))


def apply_lookback(cursor_old_iso: str, minutes: int) -> str:
    dt = parse_iso_dt(cursor_old_iso)
    if dt is None:
        return ensure_tz(cursor_old_iso)
    return (dt - timedelta(minutes=max(0, minutes))).replace(microsecond=0).isoformat()


def max_cursor_from_rows(rows: list[dict[str, Any]], fallback: str) -> str:
    best_dt: datetime | None = None
    best_raw: str | None = None
    for row in rows:
        cursor = row.get("lastChangeDate") or row.get("date")
        if not cursor:
            continue
        dt = parse_iso_dt(str(cursor))
        if dt and (best_dt is None or dt > best_dt):
            best_dt = dt
            best_raw = str(cursor)
    return ensure_tz(best_raw) if best_raw else ensure_tz(fallback)


def dedupe_by_srid(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for row in rows:
        srid = row.get("srid")
        if not srid:
            continue
        cursor = row.get("lastChangeDate") or row.get("date")
        if not cursor:
            continue
        dt = parse_iso_dt(str(cursor))
        if dt is None:
            continue
        key = str(srid)
        prev = best.get(key)
        if prev is None or dt > prev[0]:
            best[key] = (dt, row)
    return [pair[1] for pair in best.values()]
