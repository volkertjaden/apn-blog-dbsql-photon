# Databricks notebook source
# MAGIC %run "./iot_init"

# COMMAND ----------

# MAGIC %md
# MAGIC Remove all storage directories

# COMMAND ----------

# remove directories
dbutils.fs.rm(workingDir,recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop database

# COMMAND ----------

spark.sql(f'DROP DATABASE IF EXISTS {db_name} CASCADE')

# COMMAND ----------

# MAGIC %md
# MAGIC Delete Kinesis stream

# COMMAND ----------

# DBTITLE 1,Remove Kinesis stream
import boto3
import json
kinesis_client = boto3.client('kinesis', region_name=kinesisRegion)

response = kinesis_client.delete_stream(
    StreamName=my_stream_name
)

print(response)
