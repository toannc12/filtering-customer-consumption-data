DROP TABLE IF EXISTS consumption_{{ ds_nodash }};

CREATE TABLE consumption_{{ ds_nodash }} (
    Category TEXT,
    "Sub-Category" TEXT,
    Month TEXT,
    "Millions of Dollars" TEXT
)