import os
from datetime import datetime
from src.warehouse_module import DataWarehouseModule
from src.etl import main
from src.s3_module import CloudStorage

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator
)

warehouse = DataWarehouseModule()
cloude_storage = CloudStorage()


def _download_upload_file(bucket):
    files_in_processed_zone = cloude_storage.get_files(bucket)
    for key in files_in_processed_zone:
        cloude_storage.download_files(bucket, key)
        warehouse.copy_to_staging_tables(key)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 28),
    "depends_on_past": True,
    'retries': 2,
    'catchup': False,
    'provide_context': True,
}

dag = DAG(
    'Pipeline',
    default_args=default_args,
    schedule_interval="@daily",
    template_searchpath='src/sql/'
)

start_task = DummyOperator(task_id='Start', dag=dag)

main_job = PythonOperator(task_id='main_job',
                          python_callable=main,
                          dag=dag)

setup_staging_schema = PostgresOperator(
    task_id="setup_staging_schema",
    postgres_conn_id="postgres",
    sql="create_staging_schema.sql",
    dag=dag)

drop_staging_news_table = PostgresOperator(
    task_id="drop_staging_news_table",
    postgres_conn_id="postgres",
    sql="drop_staging_news_table.sql",
    dag=dag)

drop_staging_reddit_table = PostgresOperator(
    task_id="drop_staging_reddit_table",
    postgres_conn_id="postgres",
    sql="drop_staging_reddit_table.sql",
    dag=dag)

create_staging_news_table = PostgresOperator(
    task_id="create_staging_news_table",
    postgres_conn_id="postgres",
    sql="create_staging_news_table.sql",
    dag=dag)

create_staging_reddit_table = PostgresOperator(
    task_id="create_staging_reddit_table",
    postgres_conn_id="postgres",
    sql="create_staging_reddit_table.sql",
    dag=dag)

download_upload_files = PythonOperator(task_id='download_processed_files',
                                       python_callable=_download_upload_file,
                                       op_args=[os.environ.get(
                                           "PROCESSED_ZONE_BUCKET_NAME")],
                                       dag=dag)

clean_bucket = PythonOperator(task_id='clean_processed_bucket',
                              python_callable=cloude_storage.clean_bucket,
                              op_args=[os.environ.get(
                                  "PROCESSED_ZONE_BUCKET_NAME")],
                              dag=dag)

setup_warehouse_schema = PostgresOperator(
    task_id="setup_warehouse_schema",
    postgres_conn_id="postgres",
    sql="create_warehouse_schema.sql",
    dag=dag)

create_warehouse_news_table = PostgresOperator(
    task_id="create_warehouse_news_table",
    postgres_conn_id="postgres",
    sql="create_warehouse_news_table.sql",
    dag=dag)

create_warehouse_reddit_table = PostgresOperator(
    task_id="create_warehouse_reddit_table",
    postgres_conn_id="postgres",
    sql="create_warehouse_reddit_table.sql",
    dag=dag)

column_checks_for_news = SQLColumnCheckOperator(
    task_id="column_checks_for_news",
    conn_id="postgres",
    table='STAGING.news',
    column_mapping={
        "sources": {"null_check": {"equal_to": 0}},
            "titles": {"null_check": {"equal_to": 0}},
            "urls": {"null_check": {"equal_to": 0}},
    },
    dag=dag)

column_checks_for_reddit = SQLColumnCheckOperator(
    task_id="column_checks_for_reddit",
    conn_id="postgres",
    table='STAGING.reddit',
    column_mapping={
        "titles": {"null_check": {"equal_to": 0}},
            "num_comments": {"min": {"geq_to": 0}},
            "upvote_ratio": {"max": {"leq_to": 1}},
            "post_type": {"distinct_check": {"equal_to": 3}},
    },
    dag=dag)

table_checks_for_news = SQLTableCheckOperator(
    task_id="table_checks_for_news",
    conn_id="postgres",
    table='STAGING.news',
    checks={"row_count_check": {"check_statement": "COUNT(*) >= 90"}},
    dag=dag)

table_checks_for_reddit = SQLTableCheckOperator(
    task_id="table_checks_for_reddit",
    conn_id="postgres",
    table='STAGING.reddit',
    checks={
        "created_time_check": {
            "check_statement": "EXTRACT(YEAR FROM created_utc) >= 2010"
        },
    },
    dag=dag)

upsert_reddit_table = PostgresOperator(
    task_id="upsert_reddit_table",
    postgres_conn_id="postgres",
    sql="upsert_reddit_table.sql",
    dag=dag)

upsert_news_table = PostgresOperator(
    task_id="upsert_news_table",
    postgres_conn_id="postgres",
    sql="upsert_news_table.sql",
    dag=dag)

end_task = DummyOperator(task_id='End', dag=dag)

start_task >> main_job >> [setup_staging_schema, setup_warehouse_schema]
setup_staging_schema >> [drop_staging_news_table, drop_staging_reddit_table]
drop_staging_news_table >> create_staging_news_table
drop_staging_reddit_table >> create_staging_reddit_table
setup_warehouse_schema >> [
    create_warehouse_news_table, create_warehouse_reddit_table]
[create_staging_news_table, create_staging_reddit_table, create_warehouse_news_table,
    create_warehouse_reddit_table] >> download_upload_files
download_upload_files >> [column_checks_for_news,
                          column_checks_for_reddit, table_checks_for_reddit, table_checks_for_news]
[column_checks_for_news, table_checks_for_news] >> upsert_news_table
[column_checks_for_reddit, table_checks_for_reddit] >> upsert_reddit_table
[upsert_news_table, upsert_reddit_table] >> clean_bucket >> end_task
