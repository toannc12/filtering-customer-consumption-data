from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

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

file_path = Variable.get("file_path")
file_name = 'consumption_{{ ds_nodash }}'

with DAG('filtering_customer_consumption_backup',
        default_args=default_args,
        description='DAG for filtering customer consumption data',
        schedule_interval='50 11 * * *',
        # schedule_interval=None,
        catchup=False) as dag:

    # 2: Define the Task to Check for File Existence
    wait_and_check_file = FileSensor(
        task_id='wait_and_check_file',
        filepath=f'/{file_path}/{file_name}.csv',
        fs_conn_id='my_fs',
        poke_interval=300,  # check every 5 minutes 300
        timeout=900,  # timeout after 15 minutes 900
    )

    # 3: Define the Task for Data Processing, Store and Insertion
    # tasks

    create_stored_table = PostgresOperator(
        task_id='create_stored_table',
        postgres_conn_id='postgres_default',
        sql="sql/consumption.sql"
    )

    with TaskGroup(group_id='extract_data') as extract_data_task:
        process_data_task = PythonOperator(
            task_id='process_data',
            python_callable=_process_and_load_data,
            templates_dict={'file_path': f'/{file_path}/{file_name}.csv','file_path_save': f'/{file_path}/{file_name}_save.csv'}
        )

        store_data_task = PythonOperator(
            task_id='store_data',
            python_callable = _store_data,
            templates_dict={'file_path': f'/{file_path}/{file_name}_save.csv','execution_date':'{{ ds_nodash }}'}
        )

        process_data_task >> store_data_task

    # create_insert_alcoholic_table = PostgresOperator(
    #     task_id='create_insert_alcoholic_table',
    #     postgres_conn_id='postgres_default',
    #     sql="sql/consumption_alcoholic.sql",
    # )

    # create_insert_cereals_bakery_table = PostgresOperator(
    #     task_id='create_insert_cereals_bakery_table',
    #     postgres_conn_id='postgres_default',
    #     sql="sql/consumption_cereals_bakery.sql",
    # )

    # create_insert_meats_poultry_table = PostgresOperator(
    #     task_id='create_insert_meats_poultry_table',
    #     postgres_conn_id='postgres_default',
    #     sql="sql/consumption_meats_poultry.sql",
    # )

    # =================== using dynamic tasks ======================
    from dags.python.transform_load_data import create_insert_table, table_data

    data = create_insert_table(table_data=table_data)

    create_insert_dynamic_task = PostgresOperator.partial(
        task_id="create_insert_dynamic_task",
        postgres_conn_id="postgres_default",
    ).expand(sql=data)
    # ========================================================

    # 4: Check data
    from dags.python.check_data import _check_data

    check_data_task = PythonOperator.partial(
        task_id="check_data_task",
        python_callable=_check_data,
    ).expand_kwargs(
            [
                {"op_kwargs": {"table_name":"consumption_alcoholic_{{ ds_nodash }}"}},
                {"op_kwargs": {"table_name":"consumption_cereals_bakery_{{ ds_nodash }}"}},
                {"op_kwargs": {"table_name":"consumption_meats_poultry_{{ ds_nodash }}"}}
        ]
    )

    # 5: Set the Task Dependencies
    # wait_and_check_file >> process_data_task >> create_stored_table >> store_data_task >> [create_insert_alcoholic_table, create_insert_cereals_bakery_table, create_insert_meats_poultry_table]
    wait_and_check_file >> create_stored_table >> extract_data_task >> create_insert_dynamic_task >> check_data_task