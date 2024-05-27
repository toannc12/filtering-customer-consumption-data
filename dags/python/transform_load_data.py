table_data = [
        {"table_name":"consumption_alcoholic_{{ ds_nodash }}","category_name":"Alcoholic beverages"},
        {"table_name":"consumption_cereals_bakery_{{ ds_nodash }}","category_name":"Cereals and bakery products"},
        {"table_name":"consumption_meats_poultry_{{ ds_nodash }}","category_name":"Meats and poultry"}
    ]

def create_insert_table(table_data):
    sql_queries = []
    for data in table_data:
        table_name = data["table_name"]
        category_name = data["category_name"]

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
                    FROM consumption_{{{{ ds_nodash }}}}
                    WHERE Category='{category_name}';
                """
        sql_queries.append(sql)
    return sql_queries