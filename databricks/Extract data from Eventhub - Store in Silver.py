# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Import functions

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.catalog import *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS online_endpoint_logging;
# MAGIC USE online_endpoint_logging;

# COMMAND ----------

spark.readStream.table("bronze_online_endpoint_logging").createOrReplaceTempView("vw_aml_online_endpoint_logging")

# COMMAND ----------

silver_view_aml_endpoint_df = spark.sql("""
    WITH exploded AS (
select explode(Body.*)
, partition
, offset
, sequenceNumber
, enqueuedTime
FROM
vw_aml_online_endpoint_logging
)
SELECT
col.category AS category,
col.location AS location,
col.operationName AS operationName,
col.resourceId AS resourceId,
CAST(col.time as TIMESTAMP) AS time,
col.properties.METHOD AS method,
col.properties.PATH AS path,
col.properties.SUBSCRIPTION_ID AS subscriptionId,
col.properties.WORKSPACE_ID AS workspaceId,
col.properties.ENDPOINT_NAME AS endpointName,
col.properties.PROTOCOL AS protocol,
CAST(col.properties.RESPONSE_CODE AS INTEGER) AS responseCode,
col.properties.RESPONSE_CODE_REASON AS responseCodeReason,
CAST(col.properties.MODEL_STATUS_CODE AS INTEGER) AS modelStatusCode,
col.properties.MODEL_STATUS_REASON AS modelStatusReason,
CAST(col.properties.REQUEST_PAYLOAD_SIZE AS INTEGER) AS requestPayloadSize,
CAST(col.properties.RESPONSE_PAYLOAD_SIZE AS INTEGER) AS responsePayloadSize,
col.properties.USER_AGENT AS userAgent,
col.properties.X_REQUEST_ID AS xRequestId,
col.properties.X_MS_CLIENT_REQUEST_ID AS xMsClientRequestId,
col.properties.TOTAL_DURATION AS totalDuration,
col.properties.REQUEST_DURATION AS requestDuration,
col.properties.RESPONSE_DURATION AS responseDuration,
col.properties.REQUEST_THROTTLING_DELAY AS requestThrottlingDelay,
col.properties.RESPONSE_THROTTLING_DELAY AS responseThrottlingDelay,
col.properties.AUTH_TYPE AS authType,
col.properties.IDENTITY_DATA AS identityData,
partition AS partition,
offset AS offset,
sequenceNumber AS sequenceNumber,
enqueuedTime AS enqueuedTime
FROM
exploded
    """)

(silver_view_aml_endpoint_df
     .writeStream
     .format("delta")
     .option("checkpointLocation", f"silver_online_endpoint_logging")
     .option("path", f"/silver_online_endpoint_logging.delta")
     .option("mergeSchema", True)
     .outputMode("append")
     .trigger(processingTime='2 seconds')
     .table("silver_online_endpoint_logging")
     .awaitTermination())






