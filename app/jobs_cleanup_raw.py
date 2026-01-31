from dotenv import load_dotenv
import psycopg2
from db import get_dsn

def cleanup_wb_orders_raw(days: int = 14):
    dsn = get_dsn()
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from wb_orders_raw where loaded_at < now() - (%s || ' days')::interval;",
                [days],
            )
            print(f"[CLEANUP] deleted_rows={cur.rowcount}")

if __name__ == "__main__":
    load_dotenv()
    cleanup_wb_orders_raw(14)
