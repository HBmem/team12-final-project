import json
import boto3
import pymysql
import csv
import io
import time
from datetime import datetime
from decimal import Decimal
import os

# Initialize S3 client
s3_client = boto3.client('s3')

def load_db_config(logger):
    """Load database configuration from db.properties file"""
    logger("[DEBUG] Starting to load db.properties")
    
    try:
        # Look for db.properties in the same directory as this script
        properties_path = os.path.join(os.path.dirname(__file__), 'db.properties')
        
        if not os.path.exists(properties_path):
            logger("[ERROR] db.properties file not found!")
            raise Exception("Unable to find db.properties")
        
        logger("[DEBUG] db.properties file found, loading properties")
        
        # Parse the properties file
        config = {}
        with open(properties_path, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        
        # Extract database configuration from JDBC URL
        # URL format: jdbc:mysql://host:port/database
        jdbc_url = config.get('url', '')
        
        # Parse JDBC URL
        # jdbc:mysql://tcss462-project.cluster-cd0ksegcg0ma.us-east-2.rds.amazonaws.com:3306/TEST
        if jdbc_url.startswith('jdbc:mysql://'):
            jdbc_url = jdbc_url.replace('jdbc:mysql://', '')
            # Remove any query parameters
            if '?' in jdbc_url:
                jdbc_url = jdbc_url.split('?')[0]
            
            # Split host:port/database
            parts = jdbc_url.split('/')
            host_port = parts[0]
            database = parts[1] if len(parts) > 1 else 'TEST'
            
            # Split host and port
            if ':' in host_port:
                host, port = host_port.split(':')
                port = int(port)
            else:
                host = host_port
                port = 3306
        else:
            raise Exception("Invalid JDBC URL format in db.properties")
        
        db_config = {
            'host': host,
            'port': port,
            'database': database,
            'user': config.get('username', 'admin'),
            'password': config.get('password', '')
        }
        
        logger(f"[DEBUG] DB Config loaded - URL: {jdbc_url}, Username: {db_config['user']}")
        
        return db_config
        
    except Exception as e:
        logger(f"[ERROR] Failed to load db.properties: {str(e)}")
        raise

def convert_date_format(date_str):
    """Convert date from M/d/yyyy format to yyyy-MM-dd for MySQL"""
    try:
        date_obj = datetime.strptime(date_str.strip(), '%m/%d/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return date_str

def create_table(connection, logger):
    """Creates the sales_data table if it doesn't exist"""
    logger("[DEBUG] Creating sales_data table if not exists")
    
    create_table_sql = """
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
    
    with connection.cursor() as cursor:
        cursor.execute(create_table_sql)
        connection.commit()
    
    logger("[DEBUG] Table creation/verification complete")

def lambda_handler(event, context):
    """Main Lambda handler function"""
    
    # Simple logger function
    def logger(message):
        print(message)
    
    logger("[DEBUG] ========== LOAD FUNCTION STARTED ==========")
    
    # Track timing
    start_time = time.time()
    timestamps = {}
    
    # Extract request parameters
    bucket_name = event.get('bucketname')
    file_name = event.get('filename')
    
    logger(f"[DEBUG] Request received - Bucket: {bucket_name}, File: {file_name}")
    
    rows_loaded = 0
    duplicates_skipped = 0
    connection = None
    
    try:
        # Step 1: Load database configuration
        logger("[DEBUG] Step 1: Loading database configuration")
        db_config = load_db_config(logger)
        logger("[DEBUG] Step 1: COMPLETE - Database configuration loaded")
        
        # Step 2: Download CSV from S3
        logger("[DEBUG] Step 2: Starting S3 download")
        timestamps['s3_download_start'] = time.time()
        
        logger("[DEBUG] S3 client created")
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        logger("[DEBUG] S3 object retrieved")
        
        csv_content = s3_object['Body'].read().decode('utf-8')
        timestamps['s3_download_end'] = time.time()
        logger("[DEBUG] Step 2: COMPLETE - S3 download finished")
        
        # Step 3: Connect to Aurora MySQL
        logger(f"[DEBUG] Step 3: Attempting database connection to: {db_config['host']}")
        logger("[DEBUG] This may take 10-30 seconds if database is asleep...")
        timestamps['db_connection_start'] = time.time()
        
        connection = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            autocommit=False
        )
        
        logger("[DEBUG] Step 3: COMPLETE - Database connection established!")
        logger("[DEBUG] Auto-commit disabled for batch processing")
        timestamps['db_connection_end'] = time.time()
        
        # Step 4: Create table if not exists
        logger("[DEBUG] Step 4: Creating table if not exists")
        create_table(connection, logger)
        logger("[DEBUG] Step 4: COMPLETE - Table ready")
        
        # Step 5: Prepare INSERT statement
        logger("[DEBUG] Step 5: Preparing INSERT statement")
        insert_sql = """
            INSERT IGNORE INTO sales_data (
                region, country, item_type, sales_channel, order_priority,
                order_date, order_id, ship_date, units_sold, unit_price,
                unit_cost, total_revenue, total_cost, total_profit,
                order_processing_time, gross_margin
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        logger("[DEBUG] Step 5: COMPLETE - INSERT statement prepared")
        
        # Step 6: Load data
        logger("[DEBUG] Step 6: Starting data load")
        timestamps['data_load_start'] = time.time()
        
        csv_reader = csv.reader(io.StringIO(csv_content))
        header = next(csv_reader)  # Skip header
        logger(f"[DEBUG] Header line skipped: {','.join(header)}")
        
        batch = []
        batch_limit = 1000
        total_rows = 0
        line_number = 1
        
        with connection.cursor() as cursor:
            for row in csv_reader:
                line_number += 1
                
                if len(row) < 16:
                    logger(f"[WARNING] Skipping malformed line {line_number} (expected 16 fields, got {len(row)})")
                    continue
                
                try:
                    # Prepare row data
                    row_data = (
                        row[0].strip(),  # region
                        row[1].strip(),  # country
                        row[2].strip(),  # item_type
                        row[3].strip(),  # sales_channel
                        row[4].strip(),  # order_priority
                        convert_date_format(row[5]),  # order_date
                        int(row[6].strip()),  # order_id
                        convert_date_format(row[7]),  # ship_date
                        int(row[8].strip()),  # units_sold
                        float(row[9].strip()),  # unit_price
                        float(row[10].strip()),  # unit_cost
                        float(row[11].strip()),  # total_revenue
                        float(row[12].strip()),  # total_cost
                        float(row[13].strip()),  # total_profit
                        int(row[14].strip()),  # order_processing_time
                        float(row[15].strip())  # gross_margin
                    )
                    
                    batch.append(row_data)
                    total_rows += 1
                    
                    # Execute batch when limit reached
                    if len(batch) >= batch_limit:
                        logger(f"[DEBUG] Executing batch of {len(batch)} rows")
                        result = cursor.executemany(insert_sql, batch)
                        connection.commit()
                        rows_loaded += cursor.rowcount
                        logger(f"[DEBUG] Batch complete. Total loaded: {rows_loaded}")
                        batch = []
                
                except Exception as e:
                    logger(f"[ERROR] Error processing line {line_number}: {str(e)}")
            
            # Execute remaining batch
            if batch:
                logger(f"[DEBUG] Executing final batch of {len(batch)} rows")
                cursor.executemany(insert_sql, batch)
                connection.commit()
                rows_loaded += cursor.rowcount
                logger("[DEBUG] Final batch complete")
        
        timestamps['data_load_end'] = time.time()
        logger("[DEBUG] Step 6: COMPLETE - Data load finished")
        
        # Cleanup
        logger("[DEBUG] Resources closed")
        
        # Calculate duplicates
        duplicates_skipped = total_rows - rows_loaded
        
        success_message = (
            f"Successfully loaded data from {bucket_name}/{file_name}. "
            f"Rows loaded: {rows_loaded}, Duplicates skipped: {duplicates_skipped}, "
            f"Total rows processed: {total_rows}"
        )
        logger(f"[SUCCESS] {success_message}")
        
        # Calculate timing metrics
        end_time = time.time()
        total_runtime = int((end_time - start_time) * 1000)  # milliseconds
        
        # Build response similar to Java's Inspector format
        response = {
            'bucketName': bucket_name,
            'fileName': file_name,
            'rowsLoaded': rows_loaded,
            'duplicatesSkipped': duplicates_skipped,
            'totalRowsProcessed': total_rows,
            'runtime': total_runtime,
            's3DownloadStart': int((timestamps['s3_download_start'] - start_time) * 1000),
            's3DownloadEnd': int((timestamps['s3_download_end'] - start_time) * 1000),
            'dbConnectionStart': int((timestamps['db_connection_start'] - start_time) * 1000),
            'dbConnectionEnd': int((timestamps['db_connection_end'] - start_time) * 1000),
            'dataLoadStart': int((timestamps['data_load_start'] - start_time) * 1000),
            'dataLoadEnd': int((timestamps['data_load_end'] - start_time) * 1000),
            'language': 'python',
            'platform': 'AWS Lambda',
            'functionName': context.function_name,
            'functionMemory': context.memory_limit_in_mb,
            'value': success_message
        }
        
        return response
        
    except Exception as e:
        logger(f"[CRITICAL ERROR] Exception caught: {type(e).__name__}")
        logger(f"[CRITICAL ERROR] Message: {str(e)}")
        
        import traceback
        logger("[STACK TRACE START]")
        logger(traceback.format_exc())
        logger("[STACK TRACE END]")
        
        # Rollback on error
        if connection:
            try:
                logger("[DEBUG] Attempting rollback")
                connection.rollback()
                logger("[DEBUG] Rollback successful")
            except Exception as rollback_ex:
                logger(f"[ERROR] Error during rollback: {str(rollback_ex)}")
        
        return {
            'statusCode': 500,
            'loadError': str(e),
            'message': 'Load function failed'
        }
        
    finally:
        if connection:
            try:
                logger("[DEBUG] Closing database connection")
                connection.close()
                logger("[DEBUG] Database connection closed")
            except Exception as e:
                logger(f"[ERROR] Error closing connection: {str(e)}")
        
        logger("[DEBUG] ========== LOAD FUNCTION FINISHED ==========")
