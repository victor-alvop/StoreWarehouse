from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime


def extract_transform_load_customer(**kwargs):
    try:
        # Conexión a producción usando el conn_id registrado en Airflow
        prod_hook = PostgresHook(postgres_conn_id='postgres_prod')
        prod_conn = prod_hook.get_conn()
        prod_cursor = prod_conn.cursor()
        print("Conectado a base de datos de producción")

        # Conexión a warehouse
        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        dwh_conn = dwh_hook.get_conn()
        dwh_conn.autocommit = False
        dwh_cursor = dwh_conn.cursor()
        print("Conectado a data warehouse")

        # Extracción de datos
        customer_sql = "SELECT name, gender, age, city, country, id FROM customers;"
        prod_cursor.execute(customer_sql)
        data = prod_cursor.fetchall()
        print(f"Total registros: {len(data)}")

        # Carga a dim_customer
        insert_sql = """
            INSERT INTO dim_customer(name, gender, age, city, country, id)
            VALUES %s
        """
        execute_values(dwh_cursor, insert_sql, data)
        dwh_conn.commit()
        print("Inserción completada")

    except Exception as e:
        print(f"Error durante el proceso ETL: {e}")
        raise

    finally:
        prod_cursor.close()
        prod_conn.close()
        dwh_cursor.close()
        dwh_conn.close()


# Definición del DAG
with DAG(
    dag_id="dag_dim_customer",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dim", "customers", "etl"]
) as dag:

    etl_customer = PythonOperator(
        task_id="extract_transform_load_customer",
        python_callable=extract_transform_load_customer,
        provide_context=True
    )
