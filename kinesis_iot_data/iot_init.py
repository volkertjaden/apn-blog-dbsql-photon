# Databricks notebook source
# MAGIC %md
# MAGIC ###Set paths & parameters

# COMMAND ----------

workingDir = "s3://<YOUR-BUCKET-NAME>/<YOUR-PREFIX>"

raw_path = workingDir + "/raw" #Path to bronze table
bad_records_path = workingDir + "/bad_records"

bronze_path = workingDir + "/iot/bronze.delta" #Path to bronze table
checkpointPathBronze = workingDir + "/iot/bronze.checkpoint" #Checkpoint dir

silver_path = workingDir + "/iot/silver.delta" #Silver table
checkpointPathSilver = workingDir + "/iot/silver.checkpoint"

gold_path = workingDir + "/iot/gold.delta"  #Gold table
checkpointPathGold = workingDir + "/iot/gold.checkpoint"

meta_path = workingDir + "/meta" # Path to meta data on sensors

# kinesis data
my_stream_name = '<kinesis-data-stream-name>'
kinesisRegion = '<region of data-stream>'

#database name
db_name = '<name-for-glue-database>'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Kinesis stream if necessary

# COMMAND ----------

import boto3
kinesis_client = boto3.client('kinesis', region_name=kinesisRegion)

# COMMAND ----------

try:
  response = kinesis_client.describe_stream(StreamName=my_stream_name)
except:
  response = kinesis_client.create_stream(
    StreamName=my_stream_name,
    ShardCount=1
  )

# COMMAND ----------

spark.sql(f'CREATE DATABASE IF NOT EXISTS {db_name}')
spark.sql(f'USE {db_name}')

# COMMAND ----------

# define demo parameters
params = {}
params['n_sensors'] = 20 # the number of simulated sensors
params['seed'] = 754 # seed for the random number generator
params['temp_mu'] = 90 # mean temperature for normally distributed sensor readings
params['temp_sigma'] = 20 #standard deviation for normally distributed sensor readings
params['hot sensor id'] = 3 # id for sensor which runs hot
params['n_locations'] = 4 # number of plant locations
params['kinesis_partitions'] = 2 #how many Kenisis partitions to use

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create metadata tables

# COMMAND ----------

tables_collection = spark.catalog.listTables()
table_names = [table.name for table in tables_collection]

# COMMAND ----------

# locations table
if 'locations' not in table_names:
  locations_schema = "l_id INTEGER, Name STRING, City STRING,latitude FLOAT, longitude FLOAT"
 
  (spark.createDataFrame(data= [
    (0,'Munich Plant','Munich',48.135124,11.581981),
    (1,'Berlin Plant','Berlin',52.520008,13.404954),
    (2,'Stuttgart Plant','Stuttgart',48.775845,9.182932),
    (3,'Frankfurt Plant','Frankfurt',50.110924,8.682127)
  ]
                                 ,schema=locations_schema
  )
   .write
   .mode("overwrite")
   .option('path',meta_path + '/locations.delta')
   .saveAsTable('Locations')
  )

# COMMAND ----------

# sensors
if 'sensors' not in table_names:
  from scipy.stats import norm
  from random import randint, choice
  import pandas as pd

  min_a = norm.ppf(0.01, loc=params['temp_mu'], scale=params['temp_sigma'])
  max_a = norm.ppf(0.99, loc=params['temp_mu'], scale=params['temp_sigma'])

  sensor_types = pd.DataFrame.from_records(
    [
      {'st_id' : 0, 'Sensor_Type': 'Type A Sensor', 'Manufacturer': 'SensorCorp', 'min_accept_temp': min_a,'max_accept_temp': max_a},
      {'st_id' : 1, 'Sensor_Type': 'Type B Sensor', 'Manufacturer': 'Boscha GmbH', 'min_accept_temp': min_a,'max_accept_temp': max_a}
    ]
  )

  data = {'deviceId' : [i for i in range(params['n_sensors'])],
          's_st_id' : [randint(0,1) for i in range(params['n_sensors'])],
          'l_id' : [randint(0,params['n_locations']-1) for i in range(params['n_sensors'])]
         }
  sensor_pdf = pd.DataFrame(data)
  sensor_pdf = sensor_pdf.merge(sensor_types,left_on='s_st_id',right_on='st_id').drop(['s_st_id','st_id'],axis=1).sort_values('deviceId')

  (spark
   .createDataFrame(sensor_pdf)
   .write
   .mode('overwrite')
   .option('path',meta_path + '/sensors.delta')
   .saveAsTable('Sensors'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define helper functions

# COMMAND ----------

def getActiveStreams():
  try:
    return spark.streams.active
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("Unable to iterate over all active streams - using an empty set instead.")
    return []

def stopStream(s):
  try:
    print("Stopping the stream {}.".format(s.name))
    s.stop()
    print("The stream {} was stopped.".format(s.name))
  except:
    # In extream cases, this funtion may throw an ignorable error.
    print("An [ignorable] error has occured while stoping the stream.")

def stopAllStreams():
  streams = getActiveStreams()
  while len(streams) > 0:
    stopStream(streams[0])
    streams = getActiveStreams()
