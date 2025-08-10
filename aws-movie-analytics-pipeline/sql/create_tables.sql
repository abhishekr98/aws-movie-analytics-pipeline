-- Example Athena table creation
CREATE EXTERNAL TABLE movie_analysis.cast_data (
    id STRING,
    cast_id STRING,
    character STRING,
    credit_id STRING,
    gender INT,
    name STRING,
    order_num INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://movieanalysis25/cleaned/credits/cast_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');
