# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ensuring Consistency with ACID Transactions with Delta Lake
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
# MAGIC 
# MAGIC This is a companion notebook to provide a Delta Lake example against the Bike Sharing IOT data.
# MAGIC * This notebook has been tested with **Databricks Community Edition**: *6.5 ML Beta, Python 3*

# COMMAND ----------

# MAGIC %md #What is Databricks Delta?
# MAGIC ###Databricks Delta is a Unified Data Management System, designed to deliver:
# MAGIC 
# MAGIC <a href="https://ibb.co/f2p1n0f"><img src="https://i.ibb.co/c1FgCT0/delta-1.png" width="1000" alt="delta-1" border="0"></a>
# MAGIC 
# MAGIC * Scale
# MAGIC   * Cloud Storage
# MAGIC   * Decouple Compute & Storage
# MAGIC * Reliability
# MAGIC   * ACID Compliance & Data Upserts 
# MAGIC   * Schema Enforcement & Evolution
# MAGIC * Low Latency
# MAGIC   * Real-Time Streaming Ingest
# MAGIC   * Structured Streaming
# MAGIC 
# MAGIC <a href="https://ibb.co/kmryz4j"><img src="https://i.ibb.co/5Kz9Crm/delta-2.png" width="1000" alt="delta-2" border="0"></a>
# MAGIC 
# MAGIC * Perfromance
# MAGIC   * Intelligent Partitioning & Compaction
# MAGIC   * Data Skipping & Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Step 1:** Data Ingestion [Batch and Streaming]

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/iot-stream/data-device/

# COMMAND ----------

# DBTITLE 1,Set Database
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, IntegerType

# Set Database
db ='default'
spark.sql('create database if not exists ' + db)
spark.sql('use ' + db)

# Set Paramas for UserData Table
userSourcePath = '/databricks-datasets/iot-stream/data-user/'
userDestPath = '/delta/bikeSharing/userData/'
userDestName = db + '.userData'

# Set Params for event Table
eventSourcePath = '/databricks-datasets/iot-stream/data-device/'
eventDestTable = 'bikeSharing'
eventDestPath = '/delta/bikeSharing/bikeEvent/'
eventCheckpointPath = eventDestPath + '_checkpoint'

# Clean Up Code
if True:
  # Clean up UserData Table
  dbutils.fs.rm(userDestPath, True)
  spark.sql('drop table if exists ' + userDestName)
  
  dbutils.fs.rm(eventDestTable, True)
  dbutils.fs.rm(eventCheckpointPath, True)
  spark.sql('drop table if exists ' + db + '.' + eventDestTable)


# COMMAND ----------

# DBTITLE 1,Convert User Data to Delta Table (Batch)
userData = spark.read.csv(userSourcePath, header = True, inferSchema = True)
userData.write.mode("overwrite").format("delta").save(userDestPath)
spark.sql("create table if not exists " + userDestName + " using delta location '" + userDestPath + "'")

# COMMAND ----------

# MAGIC %fs ls dbfs:/delta/bikeSharing/userData/

# COMMAND ----------

# DBTITLE 1,Query Batch Delta Table
# MAGIC %sql
# MAGIC select * from userData

# COMMAND ----------

# DBTITLE 1,View Revision History
# MAGIC %sql
# MAGIC describe history userData

# COMMAND ----------

# DBTITLE 1,ETL Event Stream to Delta Table (Streaming)
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# Write Append Code
mode = 'append'

# Define schema upfront
tableSchema = spark.read.json(eventSourcePath + 'part-00000.json.gz').schema
jsonColumnSchema = F.schema_of_json(F.lit('''{"user_id": 36, "calories_burnt": 163.90000915527344, "num_steps": 3278, "miles_walked": 1.6390000581741333, "time_stamp": "2018-07-20 07:34:28.546561", "device_id": 9}'''))
  
events = spark.readStream \
  .option("maxFilesPerTrigger", 1)\
  .schema(tableSchema) \
  .json(eventSourcePath) \
  .withColumn('timestamp', F.unix_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss') .cast(TimestampType())) \
  .withColumn('value', F.from_json('value', jsonColumnSchema))

events.writeStream \
      .format('delta') \
      .outputMode(mode) \
      .option('checkpointLocation', eventCheckpointPath) \
      .table(db + '.' + eventDestTable)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikeSharing

# COMMAND ----------

# DBTITLE 1,Query Once
# MAGIC %sql
# MAGIC select count(*) from bikesharing

# COMMAND ----------

# DBTITLE 1,Query Again
# MAGIC %sql
# MAGIC select count(*) from bikesharing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 2: Query the data [Full DDL Support]

# COMMAND ----------

# DBTITLE 1,Join with Two Delta Table
# MAGIC %sql
# MAGIC SELECT * FROM bikeSharing a, userdata b WHERE a.user_id = b.userid and user_id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Adding new data [Schema Enforcement and Evolution]
# MAGIC 
# MAGIC ####How Does Schema Enforcement Work?
# MAGIC 
# MAGIC - **Cannot contain any additional columns that are not present in the target table’s schema.** Conversely, it’s OK if the incoming data doesn’t contain every column in the table – those columns will simply be assigned null values.
# MAGIC - **Cannot have column data types that differ from the column data types in the target table.** If a target table’s column contains StringType data, but the corresponding column in the DataFrame contains IntegerType data, schema enforcement will raise an exception and prevent the write operation from taking place.
# MAGIC - **Can not contain column names that differ only by case.** This means that you cannot have columns such as ‘Foo’ and  ‘foo’ defined in the same table. While Spark can be used in case sensitive or insensitive (default) mode, Delta Lake is case-preserving but insensitive when storing the schema. Parquet is case sensitive when storing and returning column information. To avoid potential mistakes, data corruption or loss issues (which we’ve personally experienced at Databricks), we decided to add this restriction.

# COMMAND ----------

# DBTITLE 1,Query User Data
# MAGIC %sql
# MAGIC select * from userdata

# COMMAND ----------

# DBTITLE 1,Try to add data with new column
from delta.tables import *

newColumnDF = userData.limit(1)\
  .withColumn('userid', F.lit('99999').cast(IntegerType()))\
  .withColumn('addr_state', F.lit('NY'))

display(newColumnDF)

# COMMAND ----------

# DBTITLE 1,Job will fail
newColumnDF.write.format("delta").mode("append").save('/delta/bikeSharing/userData/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Does Schema Evolution Work?
# MAGIC Developers can easily use schema evolution to add the new columns that were previously rejected due to a schema mismatch. Schema evolution is activated by adding  .option('mergeSchema', 'true') to your .write or .writeStream Spark command.
# MAGIC 
# MAGIC The following types of schema changes are eligible for schema evolution during table appends or overwrites:
# MAGIC 
# MAGIC - Adding new columns (this is the most common scenario)
# MAGIC - Changing of data types from NullType -> any other type, or upcasts from ByteType -> ShortType -> IntegerType
# MAGIC 
# MAGIC Other changes, which are not eligible for schema evolution, require that the schema and data are overwritten by adding **.option("mergeSchema", "true")**. For example, in the case where the column “Foo” was originally an integer data type and the new schema would be a string data type, then all of the Parquet (data) files would need to be re-written.  Those changes include:
# MAGIC 
# MAGIC - Dropping a column
# MAGIC - Changing an existing column’s data type (in place)
# MAGIC - Renaming column names that differ only by case (e.g. “Foo” and “foo”)

# COMMAND ----------

# DBTITLE 1,Turn on ability to mergeSchema
newColumnDF.write.format("delta").mode("append").option("mergeSchema", "true").save('/delta/bikeSharing/userData/')

# COMMAND ----------

# DBTITLE 1,New Column Added with default null value to all other rows
# MAGIC %sql
# MAGIC select * from userData where userid > 35

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4: Delete and Update data [CCPA Compliance]
# MAGIC 
# MAGIC #### What are Data Subject Requests?
# MAGIC 
# MAGIC One of the most operationally significant parts of the GDPR/CCPA for companies is the data subject request.  This means providing individuals with a set of enumerated rights related to their personal data including the right to:
# MAGIC 
# MAGIC - **access** (i.e., the right to know what personal data a controller or processor has about the individual),
# MAGIC - **rectification** (i.e., the right to update incorrect personal data),
# MAGIC - **erasure** (i.e., the right to be forgotten), and
# MAGIC - **portability** (i.e., the right to export personal data in a machine-readable format).

# COMMAND ----------

# DBTITLE 1,Access and Portability
# MAGIC %sql
# MAGIC select * from userData where userid = 99999

# COMMAND ----------

# DBTITLE 1,Rectification
# MAGIC %sql
# MAGIC UPDATE userData
# MAGIC SET age = 29
# MAGIC WHERE userid = 99999;
# MAGIC select * from userData where userid = 99999

# COMMAND ----------

# DBTITLE 1,Erasure (Right to be forgotten)
# MAGIC %sql
# MAGIC DELETE FROM userData where userid = 99999;
# MAGIC select * from userData where userid = 99999

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history bikesharing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from  bikesharing as of version 0

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/bikesharing/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %sh cat /dbfs/user/hive/warehouse/bikesharing/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- VACCUUM userData

# COMMAND ----------

# MAGIC %md ####Again, no update or repairs on the tables necessary.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Designing Multihop Pipelines
# MAGIC 
# MAGIC **Multi-Hop Architecture**
# MAGIC 
# MAGIC <a href="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png"><img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" alt="delta-1" border="0"></a>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC A common architecture uses tables that correspond to different quality levels in the data engineering pipeline, progressively adding structure to the data:
# MAGIC - data ingestion (“Bronze” tables)
# MAGIC - transformation/feature engineering (“Silver” tables)
# MAGIC - and machine learning training or prediction (“Gold” tables).

# COMMAND ----------

display(userData)

# COMMAND ----------

# DBTITLE 1,Create Gold Table: Stream from Delta Table
# Write Append Code
mode = 'complete'

aggDestTable = 'bikeSharingAggTable'
aggDestPath = '/delta/bikeSharing/bikeSharingAggTable/'
aggCheckpointPath = aggDestPath + '_checkpoint'

# Clean Up Code
if True:
  dbutils.fs.rm(aggDestPath, True)
  dbutils.fs.rm(aggCheckpointPath, True)
  spark.sql('drop table if exists ' + db + '.' + aggDestTable)

sdf = spark.readStream.format('delta').table(eventDestTable)
aggTable = sdf.join(userData, sdf.user_id == userData.userid) \
  .groupBy(userData.gender, userData.age)\
  .agg(F.avg('miles_walked').alias('avg_miles_walked'))

# display(aggTable)
aggTable.writeStream\
      .format('delta') \
      .outputMode(mode) \
      .option('checkpointLocation', aggCheckpointPath) \
      .table(db + '.' + aggDestTable)

# COMMAND ----------

# DBTITLE 1,Query Gold Level Tables
# MAGIC %sql
# MAGIC -- select * from bikeSharingAggTable
# MAGIC select * from userdata

# COMMAND ----------


