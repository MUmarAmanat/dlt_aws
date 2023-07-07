# Databricks notebook source
"""
Author: Muhammad Umar Amanat
Description: Notebook for creating Kinesis stream using python boto3 api
Requriements: Data should be present at dbfs:/FileStore/dlt_aws 
Version: V1
"""

# COMMAND ----------

import boto3
import random as rnd
import json


# COMMAND ----------

SCOPE_NAME = "dlt_aws_scope"
CSV_PATH = "dbfs:/FileStore/dlt_aws"
KINESIS_REGION = "us-west-2"
KINESIS_DATA_STREAM = "data-stream"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Creation

# COMMAND ----------

kinesis_client = boto3.client('kinesis', 
                              aws_access_key_id=dbutils.secrets.get(SCOPE_NAME, "aws_access_key_id"), 
                              aws_secret_access_key=dbutils.secrets.get(SCOPE_NAME, "aws_secret_access_key"),
                              region_name=KINESIS_REGION)

# COMMAND ----------

stream_list = [KINESIS_DATA_STREAM]

for s_name in stream_list:
  try:
    kinesis_client.create_stream(StreamName=s_name, StreamModeDetails={'StreamMode':'ON_DEMAND'})  
    print(f"[INFO] {s_name} created successfully")
  except Exception as e:
    print(f"[ERROR] While creating stream={s_name}, following error occured. {e}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Generator

# COMMAND ----------

cc = rnd.choice(["MX", "RU"]) ## country code

##randomly select 10 records
df = (spark
        .read
        .option("header", True)
        .option("multiline", True)
        .csv(path=f"{CSV_PATH}/{cc}videos.csv")
        .select("video_id", "trending_date", "title", "channel_title", "category_id",
                "publish_time", "views", "likes", "dislikes", "comment_count")
        .sample(withReplacement=False, fraction=1.0)
        .limit(10)
      ).toPandas()

# COMMAND ----------

for ind, i in df.iterrows():
  try:
    response = (kinesis_client
                  .put_record(StreamName=KINESIS_DATA_STREAM,
                              Data=json.dumps(i.to_dict()),
                              PartitionKey="category_id")
                ) 
  except Exception as e:
    print(f"error occured {e}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Destination Database if not exist

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dlt_aws 

# COMMAND ----------


