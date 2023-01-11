import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from yellow_taxi_ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow/')

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

workflow = DAG(
    "FHVIngestion",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019,2,1),
    end_date= datetime(2020,2,1)
)

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'fhv_{{ (execution_date.replace(day=1) - macros.timedelta(days=1)).strftime(\'%Y-%m\') }}'

with workflow:

    download_task = BashOperator(
        task_id = 'download_data',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
        max_active_tis_per_dag = 1
    )

    ingest_task = PythonOperator(
        task_id = 'ingest_data',
        python_callable = ingest_callable,
        op_kwargs = dict(
            user = PG_USER,
            password = PG_PASSWORD,
            host = PG_HOST,
            port = PG_PORT,
            db = PG_DATABASE,
            table_name = TABLE_NAME_TEMPLATE,
            parquet_file_name = OUTPUT_FILE_TEMPLATE
        ),
        max_active_tis_per_dag = 1
    )

    download_task >> ingest_task
