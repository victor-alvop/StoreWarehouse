CREATE TABLE agg_daily_sales_product (
    date_id INT REFERENCES dim_date(id),
    product_id INT REFERENCES dim_product(id),
    total_quantity INT,
    total_sales NUMERIC(12, 2),
    PRIMARY KEY (date_id, product_id)
);

CREATE TABLE agg_monthly_sales_store (
    year INT,
    month INT,
    store_id INT REFERENCES dim_store(id),
    total_quantity INT,
    total_sales NUMERIC(12, 2),
    PRIMARY KEY (year, month, store_id)
);

CREATE TABLE agg_sales_category (
    year INT,
    month INT,
    category VARCHAR(50),
    total_quantity INT,
    total_sales NUMERIC(12, 2),
    PRIMARY KEY (year, month, category)
);

CREATE TABLE agg_sales_country_channel (
    customer_country VARCHAR(50),
    sales_channel VARCHAR(20), -- 'online' or 'physical store'
    year INT,
    month INT,
    total_quantity INT,
    total_sales NUMERIC(12, 2),
    PRIMARY KEY (customer_country, sales_channel, year, month)
);

CREATE TABLE agg_frequent_customers (
    customer_id INT REFERENCES dim_customer(id),
    year INT,
    month INT,
    total_orders INT,
    total_spent NUMERIC(12, 2),
    PRIMARY KEY (customer_id, year, month)
);
