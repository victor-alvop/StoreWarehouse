-- WAREHOUSE
CREATE TABLE dim_date (
    id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT,
    month_name VARCHAR(20),
    quarter INT
);

CREATE TABLE dim_customer (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_store (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE dim_employee (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    position VARCHAR(50),
    store_id INT
);

CREATE TABLE dim_supplier (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50)
);

CREATE TABLE fact_sales (
    id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(id),
    customer_id INT REFERENCES dim_customer(id),
    sales_channel VARCHAR(20),
    quantity INT,
    total NUMERIC(10, 2)
);

CREATE TABLE fact_inventory (
    id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(id),
    product_id INT REFERENCES dim_product(id),
    store_id INT REFERENCES dim_store(id),
    current_stock INT,
    stock_in INT,
    stock_out INT
);

