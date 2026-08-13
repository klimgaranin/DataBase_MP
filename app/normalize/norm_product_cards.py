from __future__ import annotations

from typing import Any

from app.normalize.norm_ozon_stocks import normalize_product_info_item, parse_int


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _photo_urls_from_wb(photos: Any) -> list[str]:
    if not isinstance(photos, list):
        return []
    urls: list[str] = []
    for photo in photos:
        if not isinstance(photo, dict):
            continue
        url = _text(photo.get("big"))
        if not url:
            url = _text(photo.get("c516x688") or photo.get("c246x328") or photo.get("tm") or photo.get("square"))
        if url and url not in urls:
            urls.append(url)
    return urls


def normalize_wb_content_card(row: dict[str, Any]) -> dict[str, Any] | None:
    nm_id = parse_int(row.get("nmID") or row.get("nmId") or row.get("nm_id"))
    if nm_id is None:
        return None
    photos = row.get("photos") if isinstance(row.get("photos"), list) else []
    photo_urls = _photo_urls_from_wb(photos)
    vendor_code = _text(row.get("vendorCode") or row.get("vendor_code")) or str(nm_id)
    title = _text(row.get("title") or row.get("name"))
    return {
        "marketplace": "wb",
        "article": vendor_code,
        "product_id": str(nm_id),
        "marketplace_sku": nm_id,
        "product_name": title,
        "brand": _text(row.get("brand")),
        "primary_image": photo_urls[0] if photo_urls else None,
        "images": photo_urls,
        "images_count": len(photo_urls),
        "payload": row,
        "nm_id": nm_id,
        "imt_id": parse_int(row.get("imtID") or row.get("imtId")),
        "vendor_code": vendor_code,
        "subject_id": parse_int(row.get("subjectID") or row.get("subjectId")),
        "subject_name": _text(row.get("subjectName")),
        "title": title,
        "photo_big": photo_urls[0] if photo_urls else None,
        "photos_count": len(photos),
        "sizes_count": len(row.get("sizes") or []) if isinstance(row.get("sizes"), list) else 0,
        "photos": photos,
    }


def normalize_ozon_product_card(row: dict[str, Any]) -> dict[str, Any] | None:
    normalized = normalize_product_info_item(row)
    if normalized is None:
        return None
    images = row.get("images") if isinstance(row.get("images"), list) else []
    image_urls = [url for url in (_text(item) for item in images) if url]
    primary = _text(row.get("primary_image")) or (image_urls[0] if image_urls else None)
    article = normalized.get("offer_id") or str(normalized["product_id"])
    return {
        **normalized,
        "marketplace": "ozon",
        "article": article,
        "product_id": str(normalized["product_id"]),
        "marketplace_sku": normalized.get("sku"),
        "product_name": normalized.get("name"),
        "brand": None,
        "primary_image": primary,
        "images": image_urls or ([primary] if primary else []),
        "images_count": len(image_urls or ([primary] if primary else [])),
        "payload": row,
    }
