# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Define Schema for CloudTrail Logs
cloudTrailSchema = (StructType()
  .add("Records", ArrayType(
    StructType()
    .add("additionalEventData", StringType())
    .add("apiVersion", StringType())
    .add("awsRegion", StringType())
    .add("errorCode", StringType())
    .add("errorMessage", StringType())
    .add("eventID", StringType())
    .add("eventName", StringType())
    .add("eventSource", StringType())
    .add("eventTime", StringType())
    .add("eventType", StringType())
    .add("eventVersion", StringType())
    .add("readOnly", BooleanType())
    .add("recipientAccountId", StringType())
    .add("requestID", StringType())
    .add("requestParameters", MapType(StringType(), StringType()))
    .add("resources", ArrayType(StructType()
      .add("ARN", StringType())
      .add("accountId", StringType())
      .add("type", StringType())
    ))
    .add("responseElements", MapType(StringType(), StringType()))
    .add("sharedEventID", StringType())
    .add("sourceIPAddress", StringType())
    .add("serviceEventDetails", MapType(StringType(), StringType()))
    .add("userAgent", StringType())
    .add("userIdentity", StructType()
      .add("accessKeyId", StringType())
      .add("accountId", StringType())
      .add("arn", StringType())
      .add("invokedBy", StringType())
      .add("principalId", StringType())
      .add("sessionContext", StructType()
        .add("attributes", StructType()
          .add("creationDate", StringType())
          .add("mfaAuthenticated", StringType())
        )
        .add("sessionIssuer", StructType()
          .add("accountId", StringType())
          .add("arn", StringType())
          .add("principalId", StringType())
          .add("type", StringType())
          .add("userName", StringType())
        )
      )
      .add("type", StringType())
      .add("userName", StringType())
      .add("webIdFederationData", StructType()
        .add("federatedProvider", StringType())
        .add("attributes", MapType(StringType(), StringType()))
      )
    )
    .add("vpcEndpointId", StringType())))
                   )

# COMMAND ----------

# DBTITLE 1,Set up auto-loader
input_path = 's3://<CLOUDTRAIL-BUCKET-NAME>/AWSLogs/<AWS-ACC-NO>/CloudTrail/*/*/*/*'
output_path = 's3://<YOUR-BUCKET-NAME>/cloudtrail/delta/'
schema = cloudTrailSchema
checkpoint_path = 's3://<YOUR-BUCKET-NAME>/cloudtrail/checkpoints/'

# COMMAND ----------

df_log = (spark
          .readStream
          .format('cloudFiles')
          .option('cloudFiles.useNotifications','true')
          .option('cloudFiles.format','json')
          .schema(cloudTrailSchema)
          .load(input_path)
          .select(explode('records').alias('record'))
          .select(
           unix_timestamp('record.eventTime',"yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()).alias('timestamp'),
                'record.*')
         )

(df_log
 .writeStream
 .format('delta')
 .option('checkpointLocation',checkpoint_path)
 .trigger(once=True)
 .start(output_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS cloudtrail_blog;
# MAGIC USE cloudtrail_blog;
# MAGIC 
# MAGIC CREATE TABLE logdata
# MAGIC USING DELTA
# MAGIC LOCATION 's3://<YOUR-BUCKET-NAME>/cloudtrail/delta/'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from logdata

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from cloudtrail_blog.logdata
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Compact Files & Z-Order Table for faster access
# MAGIC %sql
# MAGIC OPTIMIZE cloudtrail_blog.logdata ZORDER BY (timestamp)
