import psycopg2
from psycopg2.extras import execute_values

# Connection parameters
PROD_DB_CONFIG = {
    'host': '*',
    'port': 0,
    'database': '*',
    'user': '*',
    'password': '*'
}

DWH_DB_CONFIG = {
    'host': '*',
    'port': 0,
    'database': '*',
    'user': '*',
    'password': '*'
}

def fact_sales_insertion(production_db_config, warehouse_db_config):
    try:
        #db connections
        conn_production_db = psycopg2.connect(**production_db_config)
        prod_cursor = conn_production_db.cursor()
        print('Production connection OK')

        conn_warehouse_db = psycopg2.connect(**warehouse_db_config)
        dwh_cursor = conn_warehouse_db.cursor()
        print('Warehouse connection OK')

        conn_warehouse_db.autocommit = False
        
        # Production db data extraction
        print('Fetching data...')
        sales_sql = ("""
            WITH agg_order_items AS (
                SELECT order_id, SUM(quantity) AS quantity, SUM(unit_price) AS total 
                FROM order_items 
                GROUP BY order_id 
                ORDER BY order_id ASC
            )
            SELECT o.id,
                o.order_date as date_id, 
                o.customer_id, 
                o.sales_channel, 
                oi.quantity, 
                oi.total
            FROM orders o
            LEFT JOIN agg_order_items oi ON oi.order_id = o.id;
        """)
        
        prod_cursor.execute(sales_sql)
        sales_tuple = prod_cursor.fetchall()

        # Data managment to insert customers
        sales = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in sales_tuple]
        print(f'Total records: {len(sales)}')
        print('Starting insertion...')
        sales_insertion_sql = ("""
            INSERT INTO fact_sales(id, date_id, customer_id, sales_channel, quantity, total)
            VALUES %s
        """)

        execute_values(dwh_cursor, sales_insertion_sql, sales)
        conn_warehouse_db.commit()

        print(f"\n\nValues inserted: {len(sales)}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        prod_cursor.close()
        conn_production_db.close()
        dwh_cursor.close()
        conn_warehouse_db.close()

# call function                        
fact_sales_insertion(PROD_DB_CONFIG, DWH_DB_CONFIG)    
