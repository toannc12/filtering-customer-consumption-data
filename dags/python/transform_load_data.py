import json

def create_insert_table(config_info):
    sql_queries = []
    for table_name, category in zip(config_info['table_names'], config_info["categories"]):
        sql = f"""DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} (
                    category TEXT,
                    sub_category TEXT,
                    aggregation_date DATE,
                    millions_of_dollar INTEGER,
                    pipeline_exc_datetime DATE
                );
                INSERT INTO {table_name}(category, sub_category, aggregation_date, millions_of_dollar,pipeline_exc_datetime)
                    SELECT Category, "Sub-Category", to_date(Month,'YYYY-MM-DD'), CAST("Millions of Dollars" AS INTEGER), to_date('{{{{ ds }}}}', 'YYYY-MM-DD') as pipeline_exc_datetime
                    FROM {config_info["file_name"]}
                    WHERE Category='{category}';
                """
        sql_queries.append(sql)
    return sql_queries