# Databricks notebook source
# MAGIC %run "./iot_init"

# COMMAND ----------

import boto3
kinesis_client = boto3.client('kinesis', region_name=kinesisRegion)

# COMMAND ----------

import json
import datetime
import time
import random
from random import randint, gauss, random, seed

seed(params['seed'])

while True:
  Records = []

  for sid in range(params['n_sensors']):
    rand = random()
    if rand >= 0.05: # 5% of readings are faulty
      if sid != params['hot sensor id']:
        msg = {
          "timestamp" : str(datetime.datetime.now()),
          "deviceId" : sid,
          "temperature" : gauss(params['temp_mu'],params['temp_sigma'])
        }
      else:
        msg = {
          "timestamp" : str(datetime.datetime.now()),
          "deviceId" : sid,
          "temperature" : gauss(params['temp_mu'],params['temp_sigma']) + 60.6
        }
    else:
      msg = {
      "timestamp" : str(datetime.datetime.now()),
      "deviceId" : randint(0,params['n_sensors']),
      "temperature" : 'ERROR'
    }

    Records.append(
        {
          'Data': json.dumps(msg),
          'PartitionKey': str(sid % params['kinesis_partitions'] )
        }
    )


  response = kinesis_client.put_records(
        Records=Records,
        StreamName=my_stream_name
    )
