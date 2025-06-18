from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime


def extract_transform_load_fact_sales(**kwargs):
    try:
        prod_hook = PostgresHook(postgres_conn_id='postgres_prod')
        prod_conn = prod_hook.get_conn()
        prod_cursor = prod_conn.cursor()

        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        dwh_conn = dwh_hook.get_conn()
        dwh_conn.autocommit = False
        dwh_cursor = dwh_conn.cursor()

        sales_sql = """
            WITH agg_order_items AS (
                SELECT order_id, SUM(quantity) AS quantity, SUM(unit_price) AS total 
                FROM order_items 
                GROUP BY order_id 
                ORDER BY order_id ASC
            )
            SELECT o.id,
                o.order_date as date_id, 
                o.customer_id, 
                o.sales_channel, 
                oi.quantity, 
                oi.total
            FROM orders o
            LEFT JOIN agg_order_items oi ON oi.order_id = o.id;
        """

        prod_cursor.execute(sales_sql)
        data = prod_cursor.fetchall()

        insert_sql = """
            INSERT INTO fact_sales(id, date_id, customer_id, sales_channel, quantity, total)
            VALUES %s
        """
        execute_values(dwh_cursor, insert_sql, data)
        dwh_conn.commit()

    except Exception as e:
        print(f"Error durante ETL de fact_sales: {e}")
        raise

    finally:
        prod_cursor.close()
        prod_conn.close()
        dwh_cursor.close()
        dwh_conn.close()


with DAG(
    dag_id="dag_fact_sales",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fact", "sales", "etl"]
) as dag:

    etl_fact_sales = PythonOperator(
        task_id="extract_transform_load_fact_sales",
        python_callable=extract_transform_load_fact_sales,
        provide_context=True
    )
