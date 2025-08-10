import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ---------------------------
# Glue job init
# ---------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------
# Helper to flatten JSON column
# ---------------------------
def flatten_json_column(df, column_name):
    df = df.withColumn(column_name + "_exploded", explode(col(column_name)))
    df = df.select("*", col(column_name + "_exploded.*"))
    df = df.drop(column_name, column_name + "_exploded")
    return df

# ---------------------------
# Process credits.csv
# ---------------------------
credits_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\""", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://movieanalysis25/raw/credits.csv"], "recurse": True}
).toDF()

credits_df = credits_df.withColumn("cast", from_json(col("cast"), "array<struct<cast_id:int,character:string,credit_id:string,gender:int,id:int,name:string,order:int>>"))
credits_df = credits_df.withColumn("crew", from_json(col("crew"), "array<struct<credit_id:string,department:string,gender:int,id:int,job:string,name:string>>"))

cast_df = flatten_json_column(credits_df, "cast")
crew_df = flatten_json_column(credits_df, "crew")

cast_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/credits/cast_csv/")
crew_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/credits/crew_csv/")

# ---------------------------
# Process movies_metadata.csv
# ---------------------------
movies_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\""", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://movieanalysis25/raw/movies_metadata.csv"], "recurse": True}
).toDF()

movies_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/movies_metadata_csv/")

# ---------------------------
# Process keywords.csv
# ---------------------------
keywords_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\""", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://movieanalysis25/raw/keywords.csv"], "recurse": True}
).toDF()

keywords_df = keywords_df.withColumn("keywords", from_json(col("keywords"), "array<struct<id:int,name:string>>"))
keywords_df = flatten_json_column(keywords_df, "keywords")

keywords_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/keywords_csv/")

# ---------------------------
# Process links.csv
# ---------------------------
links_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\""", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://movieanalysis25/raw/links.csv"], "recurse": True}
).toDF()

links_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/links_csv/")

# ---------------------------
# Process ratings.csv
# ---------------------------
ratings_df = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\""", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://movieanalysis25/raw/ratings.csv"], "recurse": True}
).toDF()

ratings_df.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/ratings_csv/")

# ---------------------------
# Commit job
# ---------------------------
job.commit()
