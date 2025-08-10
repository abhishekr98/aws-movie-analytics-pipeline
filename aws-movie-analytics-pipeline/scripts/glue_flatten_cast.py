import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw CSV from S3
df = spark.read.option("header", True).csv("s3://movieanalysis25/raw/credits.csv")

# Define schema for nested JSON in 'cast' column
cast_schema = StructType([
    StructField("cast_id", StringType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("order", IntegerType(), True)
])

# Parse and explode 'cast'
df_cast = df.withColumn("cast", from_json(col("cast"), cast_schema))             .withColumn("cast_exploded", explode(col("cast")))             .select(
                col("id"),
                col("cast_exploded.cast_id").alias("cast_id"),
                col("cast_exploded.character").alias("character"),
                col("cast_exploded.credit_id").alias("credit_id"),
                col("cast_exploded.gender").alias("gender"),
                col("cast_exploded.name").alias("name"),
                col("cast_exploded.order").alias("order_num")
            )

# Save as CSV back to S3
df_cast.write.mode("overwrite").option("header", True).csv("s3://movieanalysis25/cleaned/credits/cast_csv/")

job.commit()
