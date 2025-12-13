import boto3
import json
import io
import os
from decimal import Decimal
from datetime import datetime

from Inspector import Inspector   # your provided Inspector code
from sale import Sale             # Sale model above


DATE_FORMAT = "%m/%d/%Y"


def load_database_config():
    """
    Java version reads db.properties.
    In AWS Lambda, environment variables are the correct equivalent.
    """
    url = os.getenv("DB_URL")
    if not url:
        raise RuntimeError("DB_URL environment variable not set")
    return url


def parse_sale(line):
    data = line.split(",")

    return Sale(
        region=data[0],
        country=data[1],
        item_type=data[2],
        sales_channel=data[3],
        order_priority=data[4],
        order_date=datetime.strptime(data[5], DATE_FORMAT).date(),
        order_id=int(data[6]),
        ship_date=datetime.strptime(data[7], DATE_FORMAT).date(),
        units_sold=int(data[8]),
        unit_price=Decimal(data[9]),
        unit_cost=Decimal(data[10]),
        total_revenue=Decimal(data[11]),
        total_cost=Decimal(data[12]),
        total_profit=Decimal(data[13])
    )


def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()

    bucketname = event["bucketname"]
    filename = event["filename"]

    # Load DB config
    url = load_database_config()

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucketname, Key=filename)

    sales = set()

    body = io.TextIOWrapper(response["Body"], encoding="utf-8")
    for line in body:
        line = line.strip()
        if line.startswith("Region,Country,"):
            continue
        sales.add(parse_sale(line))

    # Write transformed CSV
    output = io.StringIO()
    output.write(
        "Region,Country,Item Type,Sales Channel,Order Priority,Order Date,"
        "Order ID,Ship Date,Units Sold,Unit Price,Unit Cost,"
        "Total Revenue,Total Cost,Total Profit,Order Processing Time,Gross Margin\n"
    )

    for sale in sales:
        output.write(str(sale) + "\n")

    csv_bytes = output.getvalue().encode("utf-8")

    dot = filename.rfind(".")
    new_filename = f"{filename[:dot]}-et{filename[dot:]}"

    s3.put_object(
        Bucket=bucketname,
        Key=new_filename,
        Body=csv_bytes,
        ContentType="text/plain"
    )

    response_payload = {
        "bucketname": bucketname,
        "filename": new_filename,
        "size": len(csv_bytes)
    }

    inspector.addAttribute("bucketname", bucketname)
    inspector.addAttribute("filename", new_filename)
    inspector.addAttribute("dbUrl", url)
    inspector.addAttribute("loadError", Decimal("0"))
    inspector.addAttribute("response", json.dumps(response_payload))

    inspector.inspectAllDeltas()
    return inspector.finish()