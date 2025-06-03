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

def suppliers_insertion(production_db_config, warehouse_db_config):
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
        supplier_sql = ("""
            SELECT id, name, country FROM suppliers;
        """)
        
        prod_cursor.execute(supplier_sql)
        supplier_tuple = prod_cursor.fetchall()

        # Data managment to insert employees
        suppliers = [(r[0], r[1], r[2]) for r in supplier_tuple]
        print(f'Total records: {len(suppliers)}')
        print('Starting insertion...')
        suppliers_insertion_sql = ("""
            INSERT INTO dim_supplier(id, name, country)
            VALUES %s
        """)

        execute_values(dwh_cursor, suppliers_insertion_sql, suppliers)
        conn_warehouse_db.commit()

        print(f"\n\nValues inserted: {len(suppliers)}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        prod_cursor.close()
        conn_production_db.close()
        dwh_cursor.close()
        conn_warehouse_db.close()

# call function                        
suppliers_insertion(PROD_DB_CONFIG, DWH_DB_CONFIG)


