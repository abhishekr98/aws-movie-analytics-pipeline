-- Create database if not exists
CREATE DATABASE IF NOT EXISTS movie_analysis;

-- 1. Cast Data Table
CREATE EXTERNAL TABLE IF NOT EXISTS movie_analysis.cast_data (
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

-- 2. Crew Data Table
CREATE EXTERNAL TABLE IF NOT EXISTS movie_analysis.crew_data (
    id STRING,
    credit_id STRING,
    department STRING,
    gender INT,
    job STRING,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://movieanalysis25/cleaned/credits/crew_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 3. Movies Metadata Table
CREATE EXTERNAL TABLE IF NOT EXISTS movie_analysis.movies_metadata (
    adult STRING,
    belongs_to_collection STRING,
    budget BIGINT,
    genres STRING,
    homepage STRING,
    id STRING,
    imdb_id STRING,
    original_language STRING,
    original_title STRING,
    overview STRING,
    popularity DOUBLE,
    poster_path STRING,
    production_companies STRING,
    production_countries STRING,
    release_date STRING,
    revenue BIGINT,
    runtime DOUBLE,
    spoken_languages STRING,
    status STRING,
    tagline STRING,
    title STRING,
    video STRING,
    vote_average DOUBLE,
    vote_count BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://movieanalysis25/cleaned/movies_metadata_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 4. Keywords Table
CREATE EXTERNAL TABLE IF NOT EXISTS movie_analysis.keywords (
    id STRING,
    keyword_id STRING,
    keyword_name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://movieanalysis25/cleaned/keywords_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 5. Ratings Table
CREATE EXTERNAL TABLE IF NOT EXISTS movie_analysis.ratings (
    user_id STRING,
    movie_id STRING,
    rating DOUBLE,
    timestamp BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\""
)
LOCATION 's3://movieanalysis25/cleaned/ratings_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');
