from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.clients.http_ozon_seller import OzonSellerClient


def main() -> int:
    client = OzonSellerClient()
    since = "2026-01-01T00:00:00Z"
    until = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    fbo_payload = {
        "cursor": "",
        "filter": {"since": since, "to": until},
        "limit": 10,
        "with": {"analytics_data": True, "financial_data": True},
    }
    fbo, fbo_log = client.request("ozon_posting_fbo_list_smoke", "/v3/posting/fbo/list", fbo_payload)
    fbo_result = fbo.get("result") or {}
    print(f"FBO status={fbo_log.response_status} count={len(fbo_result.get('postings') or [])} has_next={fbo_result.get('has_next')}")

    fbs_payload = {
        "filter": {"since": since, "to": until},
        "limit": 10,
        "offset": 0,
        "with": {"analytics_data": True, "financial_data": True},
    }
    fbs, fbs_log = client.request("ozon_posting_fbs_list_smoke", "/v3/posting/fbs/list", fbs_payload)
    fbs_result = fbs.get("result") or {}
    print(f"FBS status={fbs_log.response_status} count={len(fbs_result.get('postings') or [])} has_next={fbs_result.get('has_next')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
