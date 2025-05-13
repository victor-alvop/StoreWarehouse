-- Operational Database creation 
CREATE TABLE suppliers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50),
    contact VARCHAR(100)
);

SELECT * FROM suppliers;

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    unit_price NUMERIC(10, 2),
    stock INT,
    supplier_id INT REFERENCES suppliers(id)
);

SELECT * FROM products;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    country VARCHAR(50)
);

SELECT * FROM customers;

CREATE TABLE stores (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    manager_id INT 
);

SELECT * FROM stores;

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    position VARCHAR(50),
    store_id INT REFERENCES stores(id)
);

SELECT * FROM employees

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_date DATE,
    customer_id INT REFERENCES customers(id),
    total NUMERIC(10, 2),
    sales_channel VARCHAR(20)  -- 'physical_store' or 'online'
);

SELECT * FROM orders;

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(id),
    product_id INT REFERENCES products(id),
    quantity INT,
    unit_price NUMERIC(10, 2)
);

CREATE TABLE inventory_movements (
    id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(id),
    store_id INT REFERENCES stores(id),
    movement_date DATE,
    movement_type VARCHAR(20),  -- 'inbound' or 'outbound'
    quantity INT
);

SELECT * FROM inventory_movements;

