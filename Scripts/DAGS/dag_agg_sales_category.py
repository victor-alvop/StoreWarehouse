from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime


def etl_agg_daily_sales_category(**kwargs):
    try:
        prod_hook = PostgresHook(postgres_conn_id='postgres_prod')
        prod_conn = prod_hook.get_conn()
        prod_cursor = prod_conn.cursor()

        dwh_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        dwh_conn = dwh_hook.get_conn()
        dwh_conn.autocommit = False
        dwh_cursor = dwh_conn.cursor()

        sales_sql = """
            WITH date_index AS (
                SELECT order_date, id, sales_channel
                FROM orders o
            )
            SELECT d.order_date, p.category, SUM(oi.quantity) as total_quantity, SUM(oi.unit_price) as total_sales
            FROM order_items oi
            LEFT JOIN date_index d ON oi.order_id = d.id
            LEFT JOIN products p ON oi.product_id = p.id
            GROUP BY p.category, d.order_date
            ORDER BY d.order_date ASC;
        """

        prod_cursor.execute(sales_sql)
        data = prod_cursor.fetchall()

        insert_sql = """
            INSERT INTO agg_daily_sales_category(date_id, category, total_quantity, total_sales)
            VALUES %s
        """

        execute_values(dwh_cursor, insert_sql, data)
        dwh_conn.commit()

    except Exception as e:
        print(f"Error during ETL agg_daily_sales_category: {e}")
        raise

    finally:
        prod_cursor.close()
        prod_conn.close()
        dwh_cursor.close()
        dwh_conn.close()


with DAG(
    dag_id="dag_agg_daily_sales_category",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["agg", "sales", "etl"]
) as dag:

    etl_task = PythonOperator(
        task_id="etl_agg_daily_sales_category",
        python_callable=etl_agg_daily_sales_category,
        provide_context=True
    )
