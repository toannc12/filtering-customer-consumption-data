from airflow.hooks.postgres_hook import PostgresHook

def _check_data(table_names):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    record_count = hook.get_records(f"""SELECT COUNT(*) FROM {table_names}""")
    # Query to grab desired results
    df = hook.get_pandas_df(f"""SELECT * FROM {table_names}""")
    return f"Number of records in the {table_names} table: {record_count[0][0]}", df.to_csv(f'/usr/local/airflow/raw/{table_names}.csv', header=True, index=False, quoting=1)