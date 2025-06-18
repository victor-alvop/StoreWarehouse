from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime


def extract_transform_load_supplier(**kwargs):
    try:
        prod_hook = PostgresHook(postgres_conn_id='postgres_prod')
        prod_conn = prod_hook.get_conn()
        prod_cursor = prod_conn.cursor()

        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        dwh_conn = dwh_hook.get_conn()
        dwh_conn.autocommit = False
        dwh_cursor = dwh_conn.cursor()

        supplier_sql = "SELECT id, name, country FROM suppliers;"
        prod_cursor.execute(supplier_sql)
        data = prod_cursor.fetchall()

        insert_sql = """
            INSERT INTO dim_supplier(id, name, country)
            VALUES %s
        """
        execute_values(dwh_cursor, insert_sql, data)
        dwh_conn.commit()

    except Exception as e:
        print(f"Error durante ETL de proveedores: {e}")
        raise

    finally:
        prod_cursor.close()
        prod_conn.close()
        dwh_cursor.close()
        dwh_conn.close()


with DAG(
    dag_id="dag_dim_supplier",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dim", "supplier", "etl"]
) as dag:

    etl_supplier = PythonOperator(
        task_id="extract_transform_load_supplier",
        python_callable=extract_transform_load_supplier,
        provide_context=True
    )
