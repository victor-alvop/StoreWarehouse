from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime


def extract_transform_load_employee(**kwargs):
    try:
        
        prod_hook = PostgresHook(postgres_conn_id='postgres_prod')
        prod_conn = prod_hook.get_conn()
        prod_cursor = prod_conn.cursor()
        print("Conectado a base de datos de producción")

        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        dwh_conn = dwh_hook.get_conn()
        dwh_conn.autocommit = False
        dwh_cursor = dwh_conn.cursor()
        print("Conectado a data warehouse")

        employee_sql = "SELECT id, name, position, store_id FROM employees;"
        prod_cursor.execute(employee_sql)
        data = prod_cursor.fetchall()
        print(f"Total registros: {len(data)}")

        insert_sql = """
            INSERT INTO dim_employee(id, name, position, store_id)
            VALUES %s
        """
        execute_values(dwh_cursor, insert_sql, data)
        dwh_conn.commit()
        print("Inserción completada")

    except Exception as e:
        print(f"Error durante ETL de empleados: {e}")
        raise

    finally:
        prod_cursor.close()
        prod_conn.close()
        dwh_cursor.close()
        dwh_conn.close()

with DAG(
    dag_id="dag_dim_employee",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dim", "employee", "etl"]
) as dag:

    etl_employee = PythonOperator(
        task_id="extract_transform_load_employee",
        python_callable=extract_transform_load_employee,
        provide_context=True
    )
