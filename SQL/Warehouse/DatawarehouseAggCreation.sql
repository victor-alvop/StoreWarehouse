CREATE TABLE agg_daily_sales_product (
    date_id DATE REFERENCES dim_date(date),
    product_id INT REFERENCES dim_product(id),
    total_quantity INT,
    total_sales NUMERIC(12, 2),
    PRIMARY KEY (date_id, product_id)
);

CREATE TABLE agg_daily_sales_channel (
	id SERIAL PRIMARY KEY,
	date_id DATE REFERENCES dim_date(date),
    sales_channel VARCHAR(50),
    total_quantity INT,
    total_sales NUMERIC(12, 2)
);

CREATE TABLE agg_daily_sales_category (
	id SERIAL PRIMARY KEY,
	date_id DATE REFERENCES dim_date(date),
    category VARCHAR(50),
    total_quantity INT,
    total_sales NUMERIC(12, 2)
);

CREATE TABLE agg_sales_country_channel (
	id SERIAL PRIMARY KEY,
	date_id DATE REFERENCES dim_date(date),
	country VARCHAR(100),
    total_quantity INT,
    total_sales NUMERIC(12, 2)
);

CREATE TABLE agg_frequent_customers (
	id SERIAL PRIMARY KEY,
	date_id DATE REFERENCES dim_date(date),
    customer_id INT REFERENCES dim_customer(id),
    total_orders INT,
    total_spent NUMERIC(12, 2)
);
