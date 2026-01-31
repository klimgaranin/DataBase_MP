from dotenv import load_dotenv
from db import insert_raw

def main():
    load_dotenv()

    fake_order = {
        "order_id": "TEST-1",
        "created_at": "2026-01-17T02:35:00+03:00",
        "items": [{"sku": "SKU-123", "qty": 1, "price": 1000}],
    }

    insert_raw("wb_orders_raw", fake_order)
    print("OK: inserted 1 row into wb_orders_raw")

if __name__ == "__main__":
    main()
