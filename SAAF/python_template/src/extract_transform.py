import logging
import datetime
import boto3
import csv
import io
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extractTransform(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    
    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()
    
    # Check if 'bucketname' and 'filename' are in the request
    logger.info("Lambda start: request keys: %s", list(request.keys()) if isinstance(request, dict) else "<not dict>")
    if ('bucketname' in request and 'filename' in request):
        bucketname = str(request['bucketname'])
        filename = str(request['filename'])

        # Configure S3 client with sensible timeouts to avoid indefinite hangs
        s3 = boto3.client('s3', config=Config(connect_timeout=5, read_timeout=60, retries={'max_attempts': 3}))

        # Fetch the object from S3 and stream lines instead of loading whole file
        logger.info("Fetching object %s from bucket %s", filename, bucketname)
        try:
            obj = s3.get_object(Bucket=bucketname, Key=filename)
            content_length = obj.get('ContentLength')
            logger.info("get_object returned; ContentLength=%s", str(content_length))
            body = obj['Body']
            text_stream = io.TextIOWrapper(body, encoding='utf-8')
            lines_iter = text_stream
        except Exception as e:
            logger.exception("Error fetching object %s from %s", filename, bucketname)
            inspector.addAttribute("message", f"Error fetching object: {e}")
            inspector.inspectAllDeltas()
            return inspector.finish()

        # Process CSV data (streamed)
        logger.info("Processing CSV data (streamed)")
        sales = []
        seen_order_ids = set()
        for line in lines_iter:
            if not line.strip():
                continue
            # Skip header line
            if line.startswith("Region,Country,"):
                continue
            try:
                sale = parse_sale(line)
            except Exception:
                # Skip malformed lines
                logger.debug("Skipping malformed line")
                continue
            order_id = sale.get("order_id")
            if order_id in seen_order_ids:
                # Duplicate order id: ignore
                continue
            seen_order_ids.add(order_id)
            sales.append(sale)
        logger.info("Processed %d sales records", len(sales))

        # Write new CSV file
        logger.info("Writing transformed data to new CSV")
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "Region","Country","Item Type","Sales Channel","Order Priority",
            "Order Date","Order ID","Ship Date","Units Sold","Unit Price",
            "Unit Cost","Total Revenue","Total Cost","Total Profit",
            "Order Processing Time","Gross Margin"
        ])

        for sale in sales:
            writer.writerow(sale_to_list(sale))

        csv_data = output.getvalue().encode('utf-8')
        new_file_name = filename.rsplit(".", 1)[0] + "-et.csv"

        logger.info("Uploading processed file %s (size=%d bytes)", new_file_name, len(csv_data))
        try:
            s3.put_object(
                Bucket=bucketname,
                Key=new_file_name,
                Body=csv_data,
                ContentType='text/csv'
            )
        except Exception as e:
            logger.exception("Error uploading processed file %s to %s", new_file_name, bucketname)
            inspector.addAttribute("message", f"Error uploading processed file: {e}")
            inspector.inspectAllDeltas()
            return inspector.finish()

        msg_status = {
            "status": "success",
            "bucket": bucketname,
            "newFile": new_file_name,
            "size": len(csv_data)
        }
        inspector.addAttribute("message", json.dumps(msg_status))
    else:
        inspector.addAttribute("message", "Error: Missing bucketname or filename in request")
    
    inspector.inspectAllDeltas()
    return inspector.finish()

def parse_sale(line):
    cols = line.split(",")
    fmt = "%m/%d/%Y"

    def format_order_priority(priority):
        mapping = {
            "L": "Low",
            "M": "Medium",
            "H": "High",
            "C": "Critical"
        }
        return mapping.get(priority, "Unknown")
    
    return {
        "region": cols[0],
        "country": cols[1],
        "item_type": cols[2],
        "sales_channel": cols[3],
        "order_priority": format_order_priority(cols[4]),
        "order_date": datetime.datetime.strptime(cols[5], fmt),
        "order_id": int(cols[6]),
        "ship_date": datetime.datetime.strptime(cols[7], fmt),
        "units_sold": int(cols[8]),
        "unit_price": float(cols[9]),
        "unit_cost": float(cols[10]),
        "total_revenue": float(cols[11]),
        "total_cost": float(cols[12]),
        "total_profit": float(cols[13])
    }

def sale_to_list(sale):
    order_processing_time = (sale["ship_date"] - sale["order_date"]).days
    gross_margin = sale["total_profit"] / sale["total_revenue"] if sale["total_revenue"] != 0 else 0.0

    return [
        sale["region"],
        sale["country"],
        sale["item_type"],
        sale["sales_channel"],
        sale["order_priority"],
        sale["order_date"].strftime("%m/%d/%Y"),
        sale["order_id"],
        sale["ship_date"].strftime("%m/%d/%Y"),
        sale["units_sold"],
        f"{sale['unit_price']:.2f}",
        f"{sale['unit_cost']:.2f}",
        f"{sale['total_revenue']:.2f}",
        f"{sale['total_cost']:.2f}",
        f"{sale['total_profit']:.2f}",
        order_processing_time,
        f"{gross_margin:.4f}"
    ]