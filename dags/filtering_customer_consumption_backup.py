from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from python.process_stored_data import _process_and_load_data, _store_data

# 1: Define the DAG and its Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 13),
    'execution_timeout': timedelta(minutes=15),
}

dag = DAG('filtering_customer_consumption_backup',
        default_args=default_args,
        description='DAG for filtering customer consumption data',
        # schedule_interval='50 11 * * *',
        schedule_interval=None,
        catchup=False)

# 2: Define the Task to Check for File Existence
wait_and_check_file = FileSensor(
    task_id='wait_and_check_file',
    filepath='/usr/local/airflow/raw/consumption_{{ ds_nodash }}.csv',
    fs_conn_id='my_fs_conn',
    poke_interval=60,  # check every 5 minutes 300
    timeout=120,  # timeout after 15 minutes 900
    dag=dag
)

# 3: Define the Task for Data Processing, Store and Insertion
# tasks
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=_process_and_load_data,
    templates_dict={'file_path': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}.csv','file_path_save': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}_save.csv'}
)

create_stored_table = PostgresOperator(
    task_id='create_stored_table',
    postgres_conn_id='postgres_default',
    sql="sql/consumption.sql"
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable = _store_data,
    templates_dict={'file_path': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}_save.csv','execution_date':'{{ ds_nodash }}'}
)

create_insert_alcoholic_table = PostgresOperator(
    task_id='create_insert_alcoholic_table',
    postgres_conn_id='postgres_default',
    sql="sql/consumption_alcoholic.sql",
)

create_insert_cereals_bakery_table = PostgresOperator(
    task_id='create_insert_cereals_bakery_table',
    postgres_conn_id='postgres_default',
    sql="sql/consumption_cereals_bakery.sql",
)

create_insert_meats_poultry_table = PostgresOperator(
    task_id='create_insert_meats_poultry_table',
    postgres_conn_id='postgres_default',
    sql="sql/consumption_meats_poultry.sql",
)
# ===================dynamic tasks ======================
from dags.python.demo_dynamic import create_insert_table, table_data

data = create_insert_table(table_data=table_data)

create_insert_dynamic_task = PostgresOperator.partial(
    task_id="create_insert_dynamic_task",
    postgres_conn_id="postgres_default",
).expand(sql=data)

# 4: Set the Task Dependencies
# wait_and_check_file >> process_data_task >> create_stored_table >> store_data_task >> [create_insert_alcoholic_table, create_insert_cereals_bakery_table, create_insert_meats_poultry_table]
wait_and_check_file >> process_data_task >> create_stored_table >> store_data_task >> create_insert_dynamic_task