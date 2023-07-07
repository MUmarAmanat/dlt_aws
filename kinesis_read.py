# Databricks notebook source
"""
Author: Muhammad Umar Amanat
Description: Reading data from kinesis data stream and store into bronze table
Requriements: NO
Version: V1
"""

# COMMAND ----------

import boto3

from pyspark.sql import functions as F

# COMMAND ----------

SCOPE_NAME = "dlt_aws_scope"
CSV_PATH = "dbfs:/FileStore/dlt_aws"
KINESIS_REGION = "us-west-2"
KINESIS_DATA_STREAM = "data-stream"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reader

# COMMAND ----------

data_sdf = (spark
              .readStream
              .format("kinesis")
              .option("streamName", KINESIS_DATA_STREAM)
              .option("region", KINESIS_REGION)
              .option("initialPosition", 'earliest')
              .option("awsAccessKey", dbutils.secrets.get(SCOPE_NAME, "aws_access_key_id"))
              .option("awsSecretKey", dbutils.secrets.get(SCOPE_NAME, "aws_secret_access_key"))
              .load()
            )

# COMMAND ----------

data_sdf.display()
