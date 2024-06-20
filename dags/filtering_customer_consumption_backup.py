from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import json
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

config_info = json.loads(Variable.get("config_info"))

with DAG('filtering_customer_consumption_backup',
        default_args=default_args,
        description='DAG for filtering customer consumption data',
        # schedule_interval='50 11 * * *',
        schedule_interval=None,
        catchup=False) as dag:

    # 2: Define the Task to Check for File Existence
    wait_and_check_file = FileSensor(
        task_id='wait_and_check_file',
        filepath=f'/{config_info["paths"]["file_path"]}/{config_info["file_name"]}.csv',
        fs_conn_id='my_fs',
        poke_interval=30,  # check every 5 minutes 300
        timeout=60,  # timeout after 15 minutes 900
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
            templates_dict={'file_path': f'/{config_info["paths"]["file_path"]}/{config_info["file_name"]}.csv',
                            'file_path_save': f'/{config_info["paths"]["file_path"]}/{config_info["file_name"]}_save.csv'}
        )

        store_data_task = PythonOperator(
            task_id='store_data',
            python_callable = _store_data,
            templates_dict={'file_path': f'/{config_info["paths"]["file_path_save"]}/{config_info["file_name"]}_save.csv',
                            'execution_date':'{{ ds_nodash }}'}
        )

        process_data_task >> store_data_task

    # with TaskGroup(group_id='create_insert_data') as create_insert_data:
    #     create_insert_alcoholic_table = PostgresOperator(
    #         task_id='create_insert_alcoholic_table',
    #         postgres_conn_id='postgres_default',
    #         sql="sql/consumption_category.sql",
    #         params={"table_name": "alcoholic", "category": "Alcoholic beverages"}
    #     )

    #     create_insert_cereals_bakery_table = PostgresOperator(
    #         task_id='create_insert_cereals_bakery_table',
    #         postgres_conn_id='postgres_default',
    #         sql="sql/consumption_category.sql",
    #         params={"table_name": "cereals_bakery", "category": "Cereals and bakery products"}
    #     )

    #     create_insert_meats_poultry_table = PostgresOperator(
    #         task_id='create_insert_meats_poultry_table',
    #         postgres_conn_id='postgres_default',
    #         sql="sql/consumption_category.sql",
    #         params={"table_name": "meats_poultry", "category": "Meats and poultry"}
    #     )

        # create_insert_alcoholic_table, create_insert_cereals_bakery_table, create_insert_meats_poultry_table
    
    ### dynamic task use for expand ###
    with TaskGroup(group_id='transform_and_load_data') as transform_and_load_data:
        create_insert_dynamic_task = PostgresOperator.partial(
            task_id="create_insert_dynamic_task",
            sql = f'{config_info["paths"]["sql_path"]}/consumption_category.sql',
            postgres_conn_id="postgres_default",
        ).expand(
            params = [{'category': category,
                       'table_name': table} 
                       for category, table in zip(config_info['categories'], config_info["tables"])]
        )

    # =================== using dynamic tasks ======================
    # from dags.python.transform_load_data import create_insert_table

    # data = create_insert_table(config_info)

    # create_insert_dynamic_task = PostgresOperator.partial(
    #     task_id="create_insert_dynamic_task",
    #     postgres_conn_id="postgres_default",
    # ).expand(sql=data)
    # ========================================================

    # 4: Check data
    from dags.python.check_data import _check_data

    check_data_task = PythonOperator.partial(
        task_id="check_data_task",
        python_callable=_check_data,
    ).expand_kwargs(
            [
                {"op_kwargs": {"table_names":config_info["table_names"][0]}},
                {"op_kwargs": {"table_names":config_info["table_names"][1]}},
                {"op_kwargs": {"table_names":config_info["table_names"][2]}}
        ]
    )

    # 5: Set the Task Dependencies
    wait_and_check_file >> create_stored_table >> extract_data_task >> transform_and_load_data >> check_data_task
    # wait_and_check_file >> create_stored_table >> extract_data_task >> create_insert_data >> check_data_task
    # wait_and_check_file >> create_stored_table >> extract_data_task >> create_insert_dynamic_task >> check_data_task