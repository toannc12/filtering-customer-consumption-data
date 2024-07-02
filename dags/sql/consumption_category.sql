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

--Tại mỗi năm chỉ lưu trữ ra 3 tháng có có Millions of Dollars cao nhất trong năm đó (value giảm dần)
--Sắp xếp thứ thứ tổng thể thì năm nào có tổng Millions_of_Dollars (cao nhất) thì lên đầu tiên

--Create a temporary table
CREATE TEMPORARY TABLE consumption_{{ params.table_name }}_{{ ds_nodash }}_temp_table (
    category TEXT,
    sub_category TEXT,
    aggregation_date DATE,
    millions_of_dollar INTEGER,
    pipeline_exc_datetime DATE
);

--Insert the result of the SELECT query into the temporary table
INSERT INTO consumption_{{ params.table_name }}_{{ ds_nodash }}_temp_table (
    category,
    sub_category,
    aggregation_date,
    millions_of_dollar,
    pipeline_exc_datetime
)
WITH ranked AS (
    SELECT
        category,
        sub_category,
        aggregation_date,
        millions_of_dollar,
        pipeline_exc_datetime,
        ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM aggregation_date) ORDER BY millions_of_dollar DESC) AS rank_num
    FROM consumption_{{ params.table_name }}_{{ ds_nodash }}
),
year_totals AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM aggregation_date) AS year,
        SUM(millions_of_dollar) OVER (PARTITION BY EXTRACT(YEAR FROM aggregation_date)) AS total
    FROM consumption_{{ params.table_name }}_{{ ds_nodash }}
)
SELECT
    r.category,
    r.sub_category,
    r.aggregation_date,
    r.millions_of_dollar,
    r.pipeline_exc_datetime
FROM ranked r
INNER JOIN year_totals yt ON EXTRACT(YEAR FROM r.aggregation_date) = yt.year
WHERE r.rank_num <= 3
ORDER BY yt.total DESC, EXTRACT(YEAR FROM r.aggregation_date) DESC, r.millions_of_dollar DESC;

--Replace the data from the temporary table into the original table
TRUNCATE TABLE consumption_{{ params.table_name }}_{{ ds_nodash }};
INSERT INTO consumption_{{ params.table_name }}_{{ ds_nodash }} (
    category,
    sub_category,
    aggregation_date,
    millions_of_dollar,
    pipeline_exc_datetime
)
SELECT * FROM consumption_{{ params.table_name }}_{{ ds_nodash }}_temp_table;

DROP TABLE IF EXISTS consumption_{{ params.table_name }}_{{ ds_nodash }}_temp_table;