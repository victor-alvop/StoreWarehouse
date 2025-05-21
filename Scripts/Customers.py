import psycopg2
from psycopg2.extras import execute_values


# Datos de conexión (puedes moverlos a variables de entorno si lo deseas)
PROD_DB_CONFIG = {
    'host': 'switchback.proxy.rlwy.net',
    'port': *,
    'database': '*',
    'user': '*',
    'password': '*'
}

DWH_DB_CONFIG = {
    'host': 'nozomi.proxy.rlwy.net',
    'port': *,
    'database': '*',
    'user': '*',
    'password': '*'
}

def customer_insertion(production_db_config, warehouse_db_config):
    try:
        #db connections
        conn_production_db = psycopg2.connect(**production_db_config)
        prod_cursor = conn_production_db.cursor()
        print('Production connection OK')
        conn_warehouse_db = psycopg2.connect(**warehouse_db_config)
        dwh_cursor = conn_warehouse_db.cursor()
        print('Warehouse connection OK')

        conn_warehouse_db.autocommit = False
        
        print('Fetching data...')
        customer_sql = ("""
            SELECT name, gender, age, city, country, id FROM customers LIMIT 5;
        """)
        
        prod_cursor.execute(customer_sql)
        customers_tuple = prod_cursor.fetchall()

        #new tuple
        customers = [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in customers_tuple]
        print(f'Total records: {len(customers)}')
        print('Starting insertion...')
        customer_insertion_sql = ("""
            INSERT INTO dim_customer(name, gender, age, city, country, id)
            VALUES %s
        """)

        execute_values(dwh_cursor, customer_insertion_sql, customers)
        conn_warehouse_db.commit()

        print(f"\n\nValues inserted: {len(customers)}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        prod_cursor.close()
        conn_production_db.close()
        dwh_cursor.close()
        conn_warehouse_db.close()

# call function                        
customer_insertion(PROD_DB_CONFIG, DWH_DB_CONFIG)


