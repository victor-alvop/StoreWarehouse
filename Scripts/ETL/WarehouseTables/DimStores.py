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

def stores_insertion(production_db_config, warehouse_db_config):
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
        store_sql = ("""
            SELECT id, name, city, country FROM stores;
        """)
        
        prod_cursor.execute(store_sql)
        stores_tuple = prod_cursor.fetchall()

        # Data managment to insert stores
        stores = [(r[0], r[1], r[2], r[3]) for r in stores_tuple]
        print(f'Total records: {len(stores)}')
        print('Starting insertion...')
        stores_insertion_sql = ("""
            INSERT INTO dim_store(id, name, city, country)
            VALUES %s
        """)

        execute_values(dwh_cursor, stores_insertion_sql, stores)
        conn_warehouse_db.commit()

        print(f"\n\nValues inserted: {len(stores)}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        prod_cursor.close()
        conn_production_db.close()
        dwh_cursor.close()
        conn_warehouse_db.close()

# call function                        
stores_insertion(PROD_DB_CONFIG, DWH_DB_CONFIG)


