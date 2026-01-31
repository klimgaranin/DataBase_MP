from dotenv import load_dotenv
from clients.wb_statistics import iter_orders
from db import insert_raw

def run(date_from_iso: str):
    load_dotenv()

    total = 0
    for chunk in iter_orders(date_from_iso, flag=0):
        for row in chunk:
            insert_raw("wb_orders_raw", row)
        total += len(chunk)
        print(f"[JOB] inserted_total={total}")

    print(f"[JOB] done. total_inserted={total}")

if __name__ == "__main__":
    # Для теста бери небольшую дату, чтобы не ждать очень долго
    run("2026-01-16T00:00:00+03:00")
