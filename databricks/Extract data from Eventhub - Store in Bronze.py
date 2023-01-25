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

# MAGIC %md
# MAGIC 
# MAGIC Set endpoint

# COMMAND ----------

endpoint = dbutils.secrets.get(key='eventhub-endpoint', scope="keyvault")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Event hub connection config

# COMMAND ----------

conf = {}
conf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(endpoint)
streamDF = (
    spark
        .readStream
        .format("eventhubs")
        .options(**conf)
        .load()
)

# COMMAND ----------

# MAGIC %md Define schema

# COMMAND ----------

schema = StructType([
    StructField("records", ArrayType(StructType([
        StructField("category", StringType(), True),
        StructField("location", StringType(), True),
        StructField("operationName", StringType(), True),
        StructField("resourceId", StringType(), True),
        StructField("time", StringType(), True),
        StructField("properties", StructType([
            StructField("METHOD", StringType(), True),
            StructField("PATH", StringType(), True),
            StructField("SUBSCRIPTION_ID", StringType(), True),
            StructField("WORKSPACE_ID", StringType(), True),
            StructField("ENDPOINT_NAME", StringType(), True),
            StructField("PROTOCOL", StringType(), True),
            StructField("RESPONSE_CODE", StringType(), True),
            StructField("RESPONSE_CODE_REASON", StringType(), True),
            StructField("MODEL_STATUS_CODE", StringType(), True),
            StructField("MODEL_STATUS_REASON", StringType(), True),
            StructField("REQUEST_PAYLOAD_SIZE", IntegerType(), True),
            StructField("RESPONSE_PAYLOAD_SIZE", IntegerType(), True),
            StructField("USER_AGENT", StringType(), True),
            StructField("X_REQUEST_ID", StringType(), True),
            StructField("X_MS_CLIENT_REQUEST_ID", StringType(), True),
            StructField("TOTAL_DURATION", IntegerType(), True),
            StructField("REQUEST_DURATION", IntegerType(), True),
            StructField("RESPONSE_DURATION", IntegerType(), True),
            StructField("REQUEST_THROTTLING_DELAY", IntegerType(), True),
            StructField("RESPONSE_THROTTLING_DELAY", IntegerType(), True),
            StructField("AUTH_TYPE", StringType(), True),
            StructField("IDENTITY_DATA", StringType(), True)
            
        ]), True),
    ])), True)
])

# COMMAND ----------

rawDataDF = ( streamDF 
                  .withColumn("Body", from_json(col("body").cast(StringType()), schema)) 
                  .withColumn("sequenceNumber", col("sequenceNumber").cast(IntegerType())) 
                  .withColumn("enqueuedTime", col("enqueuedTime").cast(TimestampType())) 
            )

rawDataDFSchema = rawDataDF.schema

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS online_endpoint_logging;
# MAGIC USE online_endpoint_logging;

# COMMAND ----------

try:
    spark.catalog.createTable(tableName="bronze_online_endpoint_logging", schema=rawDataDFSchema)
except Exception as e:
    print(e)

# COMMAND ----------

sql_query = """
  MERGE INTO bronze_online_endpoint_logging a
  USING stream_updates b
  ON a.sequenceNumber=b.sequenceNumber AND a.partition=b.partition
  WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp 
        
    def upsert_to_delta(self, microBatchDF, batch):
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

streaming_merge = Upsert(sql_query)

# COMMAND ----------

query = (rawDataDF.writeStream
                   .foreachBatch(streaming_merge.upsert_to_delta)
                   .outputMode("update")
                   .option("checkpointLocation", f"bronze_online_endpoint_logging")
                   .trigger(processingTime='2 seconds')
                   .start())

query.awaitTermination()
