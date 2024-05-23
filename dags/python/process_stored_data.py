import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def _process_and_load_data(**kwargs):
    file_path = kwargs['templates_dict']['file_path']
    print(file_path)
    df = pd.read_csv(file_path)
    df['Month'] = pd.to_datetime(df["Month"]).dt.strftime("%Y-%m-%d")
    df.to_csv(kwargs['templates_dict']['file_path_save'], index=None, sep=';')

def _store_data(**kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    execution_date = kwargs['templates_dict']['execution_date']
    hook.copy_expert(
        sql = f"COPY consumption_{execution_date} FROM stdin WITH DELIMITER as ';'",
        filename = kwargs['templates_dict']['file_path']
    )