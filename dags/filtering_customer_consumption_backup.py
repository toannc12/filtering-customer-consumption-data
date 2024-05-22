from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
# import os
# from check_record_counts_operator import CheckRecordCountsOperator

from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from airflow.hooks.postgres_hook import PostgresHook
# import pandas as pd

# class CheckRecordCountsOperator(BaseOperator):

#     def __init__(self, table_name, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.file_path='/usr/local/airflow/raw/consumption_{{ ds_nodash }}.csv'
#         # self.execution_date = execution_date
#         self.table_name = table_name

#     def execute(self, context):
#         hook = PostgresHook(postgres_conn_id="postgres_default")
#         # Load the raw data from the CSV file
#         df = pd.read_csv(self.file_path)
#         # Count the number of records for each category in the raw data
#         raw_category_counts = df['Category'].value_counts()
             
#         for key, value in self.table_name.items():
#             record_count  = hook.get_records(f"SELECT COUNT(*) FROM {value}")
#             expected_count = raw_category_counts.get(key, 0)
#             actual_count = record_count[0][0]

#             if expected_count != actual_count:
#                 raise ValueError(f"Record count mismatch for '{key}' category. Expected: {expected_count}, Found: {actual_count}")

#         self.log.info("Record count validation successful.")

# =============================================================================== 

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
        #   schedule_interval='50 11 * * *',
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
# functions
def _process_and_load_data(**kwargs):
    file_path = kwargs['templates_dict']['file_path']
    # execution_date = kwargs['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    # categories = ['Alcoholic beverages', 'Cereals and bakery products', 'Meats and poultry']
    print(file_path)
    df = pd.read_csv(file_path)
    df['Month'] = pd.to_datetime(df["Month"]).dt.strftime("%Y-%m-%d")

    # formatted_data = []
    # for category in categories:
    #     filtered_df = df[df['Category'] == category]
    #     for _, row in filtered_df.iterrows():
    #         formatted_data.append((row['Category'], row['Sub-Category'], row['Month'], row['Millions of Dollars'], execution_date))

    # return formatted_data
    df.to_csv(kwargs['templates_dict']['file_path_save'], index=None, sep=';')

def _store_data(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    execution_date = kwargs['templates_dict']['execution_date']
    hook.copy_expert(
        sql = f"COPY consumption_{execution_date} FROM stdin WITH DELIMITER as ';'",
        filename = kwargs['templates_dict']['file_path']
    )

# tasks
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=_process_and_load_data,
    templates_dict={'file_path': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}.csv','file_path_save': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}_save.csv'}
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS consumption_{{ ds_nodash }} (
            Category TEXT,
            "Sub-Category" TEXT,
            Month TEXT,
            "Millions of Dollars" TEXT
        );
        CREATE TABLE IF NOT EXISTS consumption_alcoholic_{{ ds_nodash }} (
            category TEXT,
            sub_category TEXT,
            aggregation_date DATE,
            millions_of_dollar INTEGER,
            pipeline_exc_datetime DATE
        );
        CREATE TABLE IF NOT EXISTS consumption_cereals_bakery_{{ ds_nodash }} (
            category TEXT,
            sub_category TEXT,
            aggregation_date DATE,
            millions_of_dollar INTEGER,
            pipeline_exc_datetime DATE
        );
        CREATE TABLE IF NOT EXISTS consumption_meats_poultry_{{ ds_nodash }} (
            category TEXT,
            sub_category TEXT,
            aggregation_date DATE,
            millions_of_dollar INTEGER,
            pipeline_exc_datetime DATE
        );
    """
)

store_data_task = PythonOperator(
    task_id='store_data',
    python_callable = _store_data,
    templates_dict={'file_path': '/usr/local/airflow/raw/consumption_{{ ds_nodash }}_save.csv','execution_date':'{{ ds_nodash }}'}
)

load_data_task = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_default',
    sql="""
        INSERT INTO consumption_alcoholic_{{ ds_nodash }}(category, sub_category, aggregation_date, millions_of_dollar, pipeline_exc_datetime)
        SELECT Category, "Sub-Category", to_date(Month,'YYYY-MM-DD'), CAST("Millions of Dollars" AS INTEGER), to_date('{{ ds }}', 'YYYY-MM-DD') as pipeline_exc_datetime
        FROM consumption_{{ ds_nodash }}
        WHERE Category='Alcoholic beverages';

        INSERT INTO consumption_cereals_bakery_{{ ds_nodash }}(category, sub_category, aggregation_date, millions_of_dollar,pipeline_exc_datetime)
        SELECT Category, "Sub-Category", to_date(Month,'YYYY-MM-DD'), CAST("Millions of Dollars" AS INTEGER), to_date('{{ ds }}', 'YYYY-MM-DD') as pipeline_exc_datetime
        FROM consumption_{{ ds_nodash }}
        WHERE Category='Cereals and bakery products';

        INSERT INTO consumption_meats_poultry_{{ ds_nodash }}(category, sub_category, aggregation_date, millions_of_dollar,pipeline_exc_datetime)
        SELECT Category, "Sub-Category", to_date(Month,'YYYY-MM-DD'), CAST("Millions of Dollars" AS INTEGER), to_date('{{ ds }}', 'YYYY-MM-DD') as pipeline_exc_datetime
        FROM consumption_{{ ds_nodash }}
        WHERE Category='Meats and poultry';
    """,
    trigger_rule='all_done'
)

# check_record_counts_task = CheckRecordCountsOperator(
#     task_id='check_record_counts',
#     # file_path='/usr/local/airflow/raw/consumption_{{ ds_nodash }}.csv',
#     # execution_date='{{ ds_nodash }}',
#     # categories=['Alcoholic beverages', 'Cereals and bakery products', 'Meats and poultry']
#     table_name = {'Alcoholic beverages':'consumption_alcoholic_{{ ds_nodash }}', 'Cereals and bakery products':'consumption_cereals_bakery_{{ ds_nodash }}', 
#                       'Meats and poultry':'consumption_meats_poultry_{{ ds_nodash }}'}
# )

# 4: Set the Task Dependencies
wait_and_check_file >> process_data_task >> create_table_task >> store_data_task >> load_data_task
# wait_and_check_file >> process_data_task >> create_table_task >> store_data_task >> load_data_task >> check_record_counts_task