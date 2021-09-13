# Databricks notebook source
# 
# Bike Sharing Analysis
# An example Delta Live Tables pipeline that bike sharing data and builds some aggregate and feature engineering tables.
#
#

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark.sql("set spark.sql.legacy.timeParserPolicy=legacy")

bronze_path = "/databricks-datasets/iot-stream/data-device/"

@dlt.create_table(
  comment="The raw bike sharing dataset, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze"
  }
)
def bikeshare_raw():
#   tableSchema = spark.read.json(bronze_path + 'part-00000.json.gz').schema
  return (
    spark.read.json(bronze_path)
  )

@dlt.create_table(
  comment="Bike Share dataset with cleaned-up datatypes / column names and quality expectations.",  
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect_or_drop("user_id", "user_id <= 10000")
def bikeshare_clean():  
  jsonColumnSchema = schema_of_json( lit('''{"user_id": 36, "calories_burnt": 163.90000915527344, "num_steps": 3278, "miles_walked": 1.6390000581741333, "time_stamp": "2018-07-20 07:34:28.546561", "device_id": 9}'''))
  
  bikeshare = dlt.read("bikeshare_raw")
  bikeshare = (bikeshare
               .withColumn('timestamp', unix_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss') .cast(TimestampType()))
               .withColumn('value', from_json('value', jsonColumnSchema)) )  
  return (
    bikeshare
  )
