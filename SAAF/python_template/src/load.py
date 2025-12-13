from decimal import Decimal
import boto3
import pymysql
import csv
import io
import os
import re
from datetime import datetime
from Inspector import Inspector   # your provided Inspector code

def parse_jdbc_url(jdbc_url):
    match = re.match(
        r"jdbc:mysql://([^:/]+):(\d+)/(.+)", jdbc_url
    )
    if not match:
        raise ValueError("Invalid JDBC URL format")

    return {
        "host": match.group(1),
        "port": int(match.group(2)),
        "database": match.group(3)
    }

# ---------- Database Config ----------
def load_database_config():
    jdbc_url = os.environ["DB_URL"]
    parsed = parse_jdbc_url(jdbc_url)

    try:
        db_cfg = {
            "host": parsed["host"],
            "port": parsed["port"],
            "database": parsed["database"],
            "user": os.environ["DB_USERNAME"],
            "password": os.environ["DB_PASSWORD"],
            "autocommit": False
        }
        return db_cfg
    except Exception as e:
        raise Exception(f"Database configuration error: {str(e)}")


# ---------- CSV Helpers ----------
CSV_SPLIT_REGEX = re.compile(r',(?=(?:[^"]*"[^"]*")*[^"]*$)')


def parse_csv_line(line):
    return CSV_SPLIT_REGEX.split(line)


def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        return date_str


# ---------- Table Creation ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_data (
    region VARCHAR(100),
    country VARCHAR(100),
    item_type VARCHAR(100),
    sales_channel VARCHAR(50),
    order_priority VARCHAR(50),
    order_date DATE,
    order_id INT PRIMARY KEY,
    ship_date DATE,
    units_sold INT,
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    total_revenue DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    order_processing_time INT,
    gross_margin DECIMAL(5,4),
    INDEX idx_region (region),
    INDEX idx_country (country),
    INDEX idx_item_type (item_type),
    INDEX idx_order_priority (order_priority)
)
"""


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()

    bucket_name = event["bucketname"]
    file_name = event["filename"]

    rows_loaded = 0
    duplicates_skipped = 0
    total_rows = 0
    conn = None

    try:
        # Load DB config
        db_cfg = load_database_config()

        # ---------- S3 Download ----------
        inspector.addTimeStamp("s3DownloadStart")
        s3 = boto3.client("s3")
        s3_object = s3.get_object(Bucket=bucket_name, Key=file_name)
        csv_data = s3_object["Body"].read().decode("utf-8")
        inspector.addTimeStamp("s3DownloadEnd")

        reader = io.StringIO(csv_data)
        lines = reader.readlines()

        # ---------- DB Connection ----------
        inspector.addTimeStamp("dbConnectionStart")
        conn = pymysql.connect(**db_cfg)
        cursor = conn.cursor()
        inspector.addTimeStamp("dbConnectionEnd")

        # Create table
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()

        insert_sql = """
        INSERT IGNORE INTO sales_data (
            region, country, item_type, sales_channel, order_priority,
            order_date, order_id, ship_date, units_sold, unit_price,
            unit_cost, total_revenue, total_cost, total_profit,
            order_processing_time, gross_margin
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        batch = []
        batch_limit = 1000

        inspector.addTimeStamp("dataLoadStart")

        # Skip header
        for line in lines[1:]:
            if not line.strip():
                continue

            fields = parse_csv_line(line.strip())
            if len(fields) < 16:
                continue

            try:
                record = (
                    fields[0].strip(),
                    fields[1].strip(),
                    fields[2].strip(),
                    fields[3].strip(),
                    fields[4].strip(),
                    convert_date_format(fields[5].strip()),
                    int(fields[6]),
                    convert_date_format(fields[7].strip()),
                    int(fields[8]),
                    float(fields[9]),
                    float(fields[10]),
                    float(fields[11]),
                    float(fields[12]),
                    float(fields[13]),
                    int(fields[14]),
                    float(fields[15]),
                )

                batch.append(record)
                total_rows += 1

                if len(batch) >= batch_limit:
                    affected = cursor.executemany(insert_sql, batch)
                    conn.commit()
                    rows_loaded += affected
                    duplicates_skipped += len(batch) - affected
                    batch.clear()

            except Exception as row_err:
                context.getLogger().log(f"Row error: {row_err}")

        # Final batch
        if batch:
            affected = cursor.executemany(insert_sql, batch)
            conn.commit()
            rows_loaded += affected
            duplicates_skipped += len(batch) - affected

        full_url = f"jdbc:mysql://{db_cfg['host']}:{db_cfg['port']}/{db_cfg['database']}"

        inspector.addTimeStamp("dataLoadEnd")

        # ---------- Metrics ----------
        inspector.addAttribute("rowsLoaded", rows_loaded)
        inspector.addAttribute("duplicatesSkipped", duplicates_skipped)
        inspector.addAttribute("totalRowsProcessed", total_rows)
        inspector.addAttribute("bucketName", bucket_name)
        inspector.addAttribute("fileName", file_name)
        inspector.addAttribute("dbUrl", full_url)
        inspector.addAttribute("loadError", Decimal("0"))

    except Exception as e:
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()

    inspector.inspectAllDeltas()
    return inspector.finish()