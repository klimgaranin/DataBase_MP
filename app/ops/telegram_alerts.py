from __future__ import annotations

from dataclasses import dataclass
from html import escape
from typing import Any, Literal, Sequence


AlertStatus = Literal["OK", "FAIL"]


@dataclass(frozen=True)
class AlertMetric:
    label: str
    value: int | float | str
    unit: str = ""
    icon: str = "➡"


@dataclass(frozen=True)
class AlertWarning:
    text: str
    icon: str = "⚠️"


@dataclass(frozen=True)
class JobAlert:
    job_name: str
    timestamp: str
    status: AlertStatus
    metrics: tuple[AlertMetric, ...] = ()
    warnings: tuple[AlertWarning, ...] = ()
    error: str | None = None


def render_job_alert(alert: JobAlert) -> str:
    if alert.status == "FAIL":
        error = escape((alert.error or "unknown")[:200])
        return f"❌ {alert.job_name} | {alert.timestamp} | FAIL\n{error}"

    lines = [f"✅ {alert.job_name} | {alert.timestamp} | OK"]
    if alert.metrics:
        lines.append("")
        lines.extend(_render_metric(metric) for metric in alert.metrics)
    if alert.warnings:
        lines.append("")
        lines.extend(f"{warning.icon} {warning.text}" for warning in alert.warnings)
    return "\n".join(lines)


def sheet_rows_metric(label: str, result: Any) -> AlertMetric:
    return AlertMetric(label=label, value=int(getattr(result, "rows_count", 0) or 0), unit="строк")


def sheet_sync_warnings(results: Sequence[Any]) -> tuple[AlertWarning, ...]:
    added_rows = 0
    stale_rows = 0
    for result in results:
        sync = getattr(result, "sync", None)
        if sync is None:
            continue
        added_rows += int(getattr(sync, "added_sheet_rows", 0) or 0)
        stale_rows += int(getattr(sync, "stale_rows", 0) or 0)

    warnings: list[AlertWarning] = []
    if added_rows:
        warnings.append(AlertWarning(f"Строк листа добавлено: {added_rows}"))
    if stale_rows:
        warnings.append(AlertWarning(f"Устаревших строк очищено: {stale_rows}"))
    return tuple(warnings)


def _render_metric(metric: AlertMetric) -> str:
    unit = f" {metric.unit}" if metric.unit else ""
    return f"{metric.icon} {metric.label}: {metric.value}{unit}"
