---------
-- DML --
---------

-- Create Database --
---------------------
CREATE DATABASE superstore;

-- Create Raw Table --
----------------------
CREATE TABLE superstore_raw (
    "Row ID" SERIAL PRIMARY KEY,
    "Order ID" VARCHAR(50),
    "Order Date" DATE,
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(50),
    "Customer ID" VARCHAR(50),
    "Customer Name" VARCHAR(100),
    "Segment" VARCHAR(50),
    "Country" VARCHAR(50),
    "City" VARCHAR(50),
    "State" VARCHAR(50),
    "Postal Code" INTEGER,
    "Region" VARCHAR(50),
    "Product ID" VARCHAR(50),
    "Category" VARCHAR(50),
    "Sub-Category" VARCHAR(50),
    "Product Name" VARCHAR,
    "Sales" NUMERIC,
    "Quantity" INTEGER,
    "Discount" NUMERIC,
    "Profit" NUMERIC
);

-- Create Clean Table --
----------------------
CREATE TABLE superstore_clean (
    row_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50),
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code INTEGER,
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR,
    sales NUMERIC,
    quantity INTEGER,
    discount NUMERIC,
    profit NUMERIC
);

---------
-- DML --
---------
COPY superstore_raw
FROM '/opt/airflow/data/superstore_raw.csv' 
WITH CSV HEADER DELIMITER ',' ENCODING 'latin1';