import os
import json
import pymysql  # pip install pymysql
from urllib.parse import urlparse
from decimal import Decimal
from Inspector import Inspector  # your Inspector class

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


def run_queries(conn):
    try:
        with conn.cursor() as cur:
            total_revenue_by_region(cur)
            avg_gross_margin_by_region(cur)
            avg_order_processing_time(cur)
            top5_countries_by_profit(cur)
    finally:
        if conn:
            conn.close()

def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()

    conn = None
    try:
        # Load DB config from environment variables
        DB_URL = os.environ['DB_URL']
        DB_USER = os.environ['DB_USERNAME']
        DB_PASSWORD = os.environ['DB_PASSWORD']

        # Parse DB_URL robustly (supports credentials and IPv6)
        # Examples supported: mysql://host:port/dbname, mysql://user:pass@host:port/dbname, host/dbname
        # Normalize JDBC-style URLs (e.g. jdbc:mysql://host:port/db) by removing the 'jdbc:' prefix
        normalized = DB_URL
        if normalized.startswith('jdbc:'):
            normalized = normalized[len('jdbc:'):]
        parsed = urlparse(normalized if normalized.startswith("mysql://") else f"//{normalized}")
        # urlparse on a bare host/dbname requires scheme-less parsing; we prefix '//' to treat it as netloc
        host = parsed.hostname
        raw_port = parsed.port
        db_name = (parsed.path or "").lstrip('/')

        # Coerce/validate port. urlparse.port is normally int or None, but be defensive.
        port = None
        try:
            if isinstance(raw_port, int):
                port = raw_port
            elif raw_port is None:
                port = 3306
            else:
                # unexpected type (string?) attempt cast
                port = int(raw_port)
        except Exception:
            # fallback: attempt to parse netloc for host:port (after removing credentials)
            netloc = parsed.netloc or ''
            if '@' in netloc:
                netloc = netloc.split('@', 1)[1]
            # handle IPv6 [addr]:port
            if netloc.startswith('['):
                # split at ]
                try:
                    end = netloc.index(']')
                    host = netloc[:end+1]
                    rest = netloc[end+1:]
                    if rest.startswith(':'):
                        port_str = rest[1:]
                    else:
                        port_str = ''
                except ValueError:
                    port_str = ''
            else:
                parts = netloc.rsplit(':', 1)
                if len(parts) == 2:
                    host_candidate, port_str = parts
                    host = host_candidate
                else:
                    port_str = ''
            try:
                port = int(port_str) if port_str else 3306
            except Exception:
                port = 3306

        if not host or not db_name:
            raise ValueError(f"Invalid DB_URL format: {DB_URL}")

        # Connect to Aurora MySQL
        inspector.addTimeStamp("dbConnectionStart")
        conn = pymysql.connect(
            host=host,
            port=port,
            user=DB_USER,
            password=DB_PASSWORD,
            db=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        inspector.addTimeStamp("dbConnectionEnd")
        print(f"[Query] Connected to Aurora MySQL: {DB_URL}")

        # Run analytical queries
        inspector.addTimeStamp("queryStart")
        try:
            run_queries(conn)
            inspector.addAttribute("queryError", None)
        except Exception as e:
            inspector.addAttribute("queryError", str(e))
            print(f"ERROR (Query): {e}")
        inspector.addTimeStamp("queryEnd")

        # Build simple response
        response = {
            "message": f"Query service completed successfully against: {DB_URL}",
            "bucketName": event.get("bucketname"),
            "fileName": event.get("filename"),
            "dbUrl": DB_URL,
            "loadError": Decimal(0)
        }

        # Add response to Inspector
        for k, v in response.items():
            inspector.addAttribute(k, v)

    except Exception as e:
        inspector.addAttribute("queryError", str(e))
        print(f"ERROR (Query): {e}")

    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"Error closing connection: {e}")

    inspector.inspectAllDeltas()
    return inspector.finish()
