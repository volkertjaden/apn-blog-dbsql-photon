# Databricks notebook source
# MAGIC %md ---
# MAGIC # Streaming IoT Data into your AWS Lakehouse

# COMMAND ----------

# MAGIC %run "./iot_init"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Step 0: Set-up Sensor data stream 
# MAGIC - <a href="$./gen_iot_stream">Link to notebook</a><br>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 1: Set up Stream for raw data und write them into Bronze-Table

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json

sensor_schema = StructType()\
  .add('timestamp',TimestampType()) \
  .add('deviceId',IntegerType()) \
  .add('temperature',DoubleType())

sensor_readings = (spark
  .readStream
  .format("kinesis")
  .option("streamName", my_stream_name)
  .option("initialPosition", "latest")
  .option("region", kinesisRegion)
  .load()
  .withColumn('payload',col("data").cast(StringType()))
  .withColumn('json',from_json(col('payload'),sensor_schema))
  )

# COMMAND ----------

# DBTITLE 1,Let us have a first look 
display(sensor_readings.select(col('payload'),col('json.*')))

# COMMAND ----------

# DBTITLE 1,Create temporary view for SQL-Querying
sensor_readings.createOrReplaceTempView('sensor_readings')

# COMMAND ----------

# DBTITLE 1,Let us have a first look at the data (in SQL)
# MAGIC %sql
# MAGIC 
# MAGIC select json.deviceId, min(json.temperature) as min, max(json.temperature) as max, avg(json.temperature) as mean
# MAGIC from sensor_readings
# MAGIC group by deviceId
# MAGIC order by deviceId asc

# COMMAND ----------

# MAGIC %md
# MAGIC One sensor seems to have much higher readings than all the others. But to determine whether these readings are abnormal, we will need more contextual data to put into perspective

# COMMAND ----------

# DBTITLE 1,Write data to bronze table filtering out erroneous data to separate error queue
def to_bronze_table(df,epoch_id):
  df.persist()
  # Write correct data to bronze path
  (df
   .filter('json.temperature is not null')
   .select('json.*')
   .write
   .mode("append")
   .format('delta')
   .save(bronze_path)
  )
  
  # Write erroneous data to separate delta table as error queue
  (df
   .filter('json.temperature is null')
   .write
   .mode("append")
   .format('delta')
   .save(bad_records_path)
  )
  
  df.unpersist()

sensor_readings.writeStream.foreachBatch(to_bronze_table).start()

# COMMAND ----------

# DBTITLE 1,A look into the bronze-path
display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# DBTITLE 1,All changes are persisted in Delta log
display(dbutils.fs.ls(f"{bronze_path}/_delta_log"))

# COMMAND ----------

# DBTITLE 1,Let us have a look at the error queue as well
display(dbutils.fs.ls(bad_records_path))

# COMMAND ----------

display(spark.sql(f'select * from delta.`{bad_records_path}`'))

# COMMAND ----------

# DBTITLE 1,Register the Bronze Table in Hive-Metastore
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS iot_bronze
  USING DELTA
  LOCATION '{bronze_path}'
  """
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 2: Create Silver Table with added metadata

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS iot_silver
  (l_id LONG, deviceId INTEGER, timestamp TIMESTAMP, temperature DOUBLE, sensor_type STRING, manufacturer STRING,
  min_accept_temp DOUBLE, max_accept_temp DOUBLE, name STRING, city STRING, latitude FLOAT, longitude FLOAT,
  temp_high_alert BOOLEAN, temp_low_alert BOOLEAN
  )
  USING DELTA
  LOCATION '{silver_path}'
"""
)

# COMMAND ----------

# DBTITLE 1,Live-Join on the Stream und adding additional columns
from pyspark.sql.functions import col

locations = spark.read.table('locations')
sensors = spark.read.table('sensors')

silver_stream = (spark.readStream
                 .format('Delta')
                 .load(bronze_path)
                 .join(sensors, on=['deviceId'])
                 .join(locations, on=['l_id'])
                 .withColumn('Temp_high_alert', col('temperature') > col('max_accept_temp'))
                 .withColumn('Temp_low_alert', col('temperature') < col('min_accept_temp'))
                 .writeStream
                 .format("delta")
                 .option("checkpointLocation",checkpointPathSilver)
                 .outputMode("append")
                 .queryName('silver_query')
                 .table('iot_silver')
                )

# COMMAND ----------

# DBTITLE 1,Z-Order Silver table for improved read-performance on timestamp
# MAGIC %sql
# MAGIC OPTIMIZE iot_silver ZORDER BY timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from iot_silver 
# MAGIC limit 20

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Step 3: Create Gold Table to feed the monitoring dashboard

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS iot_gold
  (start timestamp, end timestamp, deviceId integer, name string, latitude float, longitude float, 
  mean_temp double, event_no long, alert_no long, alert_share double
  )
  USING DELTA
  LOCATION '{gold_path}'
"""
)

# COMMAND ----------

from pyspark.sql.functions import window, avg, count, sum

gold_stream = (spark.readStream
               .format('delta')
               .load(silver_path)
               .withWatermark('timestamp','1 minutes')
               .withColumn('in_alert',(col('Temp_high_alert') == True) | (col('Temp_low_alert') == True))
               .withColumn('in_alert',col('in_alert').cast(IntegerType()))
               .groupBy(window('timestamp','60 seconds'),'deviceId','name','latitude','longitude')
               .agg(avg('temperature').alias('mean_temp'),
                    count('*').alias('event_no'),
                    sum('in_alert').alias('alert_no')
                   )
               .withColumn('alert_share',col('alert_no')/col('event_no'))
               .select(col('window.start').alias('start'), 
                       col('window.end').alias('end'), 'deviceId','name','latitude','longitude',
                       'mean_temp', 'event_no', 'alert_no', 'alert_share')
               .writeStream
               .option("checkpointLocation",checkpointPathGold)
               .outputMode("append")
               .queryName('gold_query')
               .table('iot_gold')
              )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from iot_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_gold ZORDER BY start, end, deviceId

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Run cleanup
# MAGIC Will
# MAGIC - stop all streams
# MAGIC - drop databases and clean-up storage

# COMMAND ----------

stopAllStreams()

# COMMAND ----------

# MAGIC %run "./cleanup_iot"
