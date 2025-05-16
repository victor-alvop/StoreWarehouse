import psycopg2

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

def connection(production_db_config, warehouse_db_config):
    try:
        production_db = psycopg2.connect(**production_db_config)
        warehouse_db = psycopg2.connect(**warehouse_db_config)
        prod_cursor = production_db.cursor()
        dwh_cursor = warehouse_db.cursor()

        prod_cursor.execute("SELECT 1;")
        prod_result = prod_cursor.fetchone()

        dwh_cursor.execute("SELECT 1;")
        dwh_result = dwh_cursor.fetchone()

        if  prod_result and prod_result[0] == 1:
            print('¡Successful production conection!')

        if  prod_result and prod_result[0] == 1:
            print('¡Successful warehouse conection!')

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Cerrar todo con validación previa
        if 'prod_cursor' in locals():
            prod_cursor.close()
        if 'production_db' in locals():
            production_db.close()
        if 'dwh_cursor' in locals():
            dwh_cursor.close()
        if 'warehouse_db' in locals():
            warehouse_db.close()
            
connection(PROD_DB_CONFIG, DWH_DB_CONFIG)


