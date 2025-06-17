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

def dim_product(production_db_config, warehouse_db_config):
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
        product_sql = ("""
            select id, name, category, supplier_id from products;
        """)
        
        prod_cursor.execute(product_sql)
        product_tuple = prod_cursor.fetchall()

        # Data managment to insert products
        products = [(r[0], r[1], r[2], r[3]) for r in product_tuple]
        print(f'Total records: {len(products)}')
        print('Starting insertion...')
        sales_insertion_sql = ("""
            INSERT INTO dim_product(id, name, category, supplier_id)
            VALUES %s
        """)

        execute_values(dwh_cursor, sales_insertion_sql, products)
        conn_warehouse_db.commit()

        print(f"\n\nValues inserted: {len(products)}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        prod_cursor.close()
        conn_production_db.close()
        dwh_cursor.close()
        conn_warehouse_db.close()

# call function                        
dim_product(PROD_DB_CONFIG, DWH_DB_CONFIG) 