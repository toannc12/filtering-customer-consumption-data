DROP TABLE IF EXISTS consumption_{{ params.table_name }}_{{ ds_nodash }};

CREATE TABLE consumption_{{ params.table_name }}_{{ ds_nodash }} (
    category TEXT,
    sub_category TEXT,
    aggregation_date DATE,
    millions_of_dollar INTEGER,
    pipeline_exc_datetime DATE
);

INSERT INTO consumption_{{ params.table_name }}_{{ ds_nodash }}(category, sub_category, aggregation_date, millions_of_dollar, pipeline_exc_datetime)
    SELECT 
        Category, "Sub-Category", 
        to_date(Month,'YYYY-MM-DD'), 
        CAST("Millions of Dollars" AS INTEGER), 
        to_date('{{ ds }}', 'YYYY-MM-DD') as pipeline_exc_datetime
    FROM consumption_{{ ds_nodash }}
    WHERE Category='{{ params.category }}';