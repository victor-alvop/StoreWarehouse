import psycopg2
from psycopg2.extras import execute_values

# Datos de conexión (puedes moverlos a variables de entorno si lo deseas)
PROD_DB_CONFIG = {
    'host': 'switchback.proxy.rlwy.net',
    'port': 11021,
    'database': 'railway',
    'user': 'postgres',
    'password': 'VAdwtDLwtvKEdxQWUoBZYgZeQlfhGwJx'
}

DWH_DB_CONFIG = {
    'host': 'nozomi.proxy.rlwy.net',
    'port': 56031,
    'database': 'railway',
    'user': 'postgres',
    'password': 'CMytsZvTJJHWKEizICLLYvfbXzBKYyeo'
}

def agg_daily_sales_product(production_db_config, warehouse_db_config):
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
            WITH date_index AS (
                SELECT order_date, id
                FROM orders o
            )
            SELECT d.order_date, oi.product_id, SUM(oi.quantity) as total_quantity, SUM(oi.unit_price) as total_sales
            FROM order_items oi
            LEFT JOIN date_index d ON oi.order_id = d.id
            GROUP BY product_id, d.order_date;
        """)
        
        prod_cursor.execute(sales_sql)
        sales_tuple = prod_cursor.fetchall()

        # Data managment to insert sales
        sales = [(r[0], r[1], r[2], r[3]) for r in sales_tuple]
        print(f'Total records: {len(sales)}')
        print('Starting insertion...')
        sales_insertion_sql = ("""
            INSERT INTO agg_daily_sales_product(date_id, product_id, total_quantity, total_sales)
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
agg_daily_sales_product(PROD_DB_CONFIG, DWH_DB_CONFIG) 