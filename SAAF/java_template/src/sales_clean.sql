CREATE TABLE IF NOT EXISTS sales_data (
    region VARCHAR(100),
    country VARCHAR(100),
    item_type VARCHAR(100),
    sales_channel VARCHAR(50),
    order_priority VARCHAR(50),
    order_date VARCHAR(50),
    order_id INT PRIMARY KEY,
    ship_date VARCHAR(50),
    units_sold INT,
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    total_revenue DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    total_profit DECIMAL(12,2),
    order_processing_time INT,
    gross_margin DECIMAL(5,4)
);
