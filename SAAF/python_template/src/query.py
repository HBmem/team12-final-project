import os
import time
import pymysql


def get_connection():
    """
    Connect to Aurora MySQL using env vars:
      DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD
    """
    endpoint = os.getenv("DB_ENDPOINT")
    db_name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([endpoint, db_name, user, password]):
        raise RuntimeError("Missing DB env vars (DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD)")

    print(f"[PyQuery] Connecting to {endpoint}, DB={db_name}")

    conn = pymysql.connect(
        host=endpoint,
        user=user,
        password=password,
        database=db_name,
        port=3306,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


def total_revenue_by_region(cur):
    sql = """
        SELECT region, SUM(total_revenue) AS total_revenue
        FROM sales_data
        GROUP BY region
    """
    print("\n=== PY: TOTAL REVENUE BY REGION ===")
    cur.execute(sql)
    for row in cur.fetchall():
        print(f"Region: {row['region']:<20}  Total Revenue: {row['total_revenue']:.2f}")


def avg_gross_margin_by_region(cur):
    sql = """
        SELECT region, AVG(gross_margin) AS avg_gm
        FROM sales_data
        GROUP BY region
    """
    print("\n=== PY: AVERAGE GROSS MARGIN BY REGION ===")
    cur.execute(sql)
    for row in cur.fetchall():
        print(f"Region: {row['region']:<20}  Avg GM: {row['avg_gm']:.4f}")


def avg_order_processing_time(cur):
    sql = "SELECT AVG(order_processing_time) AS avg_opt FROM sales_data"
    print("\n=== PY: AVG ORDER PROCESSING TIME ===")
    cur.execute(sql)
    row = cur.fetchone()
    if row and row["avg_opt"] is not None:
        print(f"Average Days: {row['avg_opt']:.2f}")
    else:
        print("Average Days: 0.00")


def top5_countries_by_profit(cur):
    sql = """
        SELECT country, SUM(total_profit) AS total_profit
        FROM sales_data
        GROUP BY country
        ORDER BY total_profit DESC
        LIMIT 5
    """
    print("\n=== PY: TOP 5 COUNTRIES BY TOTAL PROFIT ===")
    cur.execute(sql)
    for row in cur.fetchall():
        print(f"Country: {row['country']:<20}  Total Profit: {row['total_profit']:.2f}")


def run_queries():
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            total_revenue_by_region(cur)
            avg_gross_margin_by_region(cur)
            avg_order_processing_time(cur)
            top5_countries_by_profit(cur)
    finally:
        if conn:
            conn.close()


def lambda_handler(event, context):
    start = time.time()
    try:
        run_queries()
        end = time.time()
        runtime_ms = int((end - start) * 1000)
        return {
            "status": "SUCCESS",
            "runtime_ms": runtime_ms
        }
    except Exception as e:
        print(f"[PyQuery] ERROR: {e}")
        return {
            "status": "ERROR",
            "message": str(e)
        }


if __name__ == "__main__":
    t0 = time.time()
    try:
        run_queries()
    except Exception as exc:
        print(f"[PyQuery] Local ERROR: {exc}")
    t1 = time.time()
    print(f"\n[PyQuery] TOTAL RUNTIME: {int((t1 - t0) * 1000)} ms")
