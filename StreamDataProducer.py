# Databricks notebook source
"""
Author: Muhammad Umar Amanat
Description: Notebook for creating Kinesis string using python boto3 api
Requriements: No 
Version: V1
"""

# COMMAND ----------

import boto3

# COMMAND ----------

kinesis_client = boto3.client('kinesis', 
                              aws_access_key_id=dbutils.secrets.get("DemoScope1", "aws_access_key_id"), 
                              aws_secret_access_key=dbutils.secrets.get("DemoScope1", "aws_secret_access_key"),
                              region_name="us-west-2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Creation

# COMMAND ----------

stream_list = ["ecom-wish-list", "ecom-stream"]
# kinesis_client.update_stream_mode(f"dbutils.secrets.get("DemoScope1", "stream_arn")/s_name" )

for s_name in stream_list:
  try:
    kinesis_client.create_stream(StreamName=s_name, StreamModeDetails={'StreamMode':'ON_DEMAND'})
    
      
    print(f"[INFO] {s_name} created successfully")
  except Exception as e:
    print(f"[ERR] While creating stream={s_name}, following error occured. {e}")

# COMMAND ----------


