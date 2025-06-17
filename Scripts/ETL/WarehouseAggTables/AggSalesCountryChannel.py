import psycopg2
from psycopg2.extras import execute_values

# Datos de conexión (puedes moverlos a variables de entorno si lo deseas)
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
                SELECT o.order_date, o.id, c.country
                FROM orders o
                LEFT JOIN customers c ON o.customer_id = c.id
            )
            SELECT d.order_date, d.country, SUM(oi.quantity) as total_quantity, SUM(oi.unit_price) as total_sales
            FROM order_items oi
            LEFT JOIN date_index d ON oi.order_id = d.id
            GROUP BY d.country, d.order_date
            ORDER BY d.order_date ASC;
        """)
        
        prod_cursor.execute(sales_sql)
        sales_tuple = prod_cursor.fetchall()

        # Data managment to insert sales
        sales = [(r[0], r[1], r[2], r[3]) for r in sales_tuple]
        print(f'Total records: {len(sales)}')
        print('Starting insertion...')
        sales_insertion_sql = ("""
            INSERT INTO agg_sales_country_channel(date_id, country, total_quantity, total_sales)
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