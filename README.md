# Delta Live table + AWS Kinesis

This repository contains code for producing data for kinesis and then reading from AWS Kinesis using Databricks Delta Live Tables.

## Requirements
1. AWS credentials and sufficient rights for creating a Kinesis data stream
2. Databricks Premium Tier account
3. Databricks rights for running delta live tables
4. Youtube Data from kaggle (https://www.kaggle.com/datasets/datasnaek/youtube-new)


## How to run?
1. Clone https://github.com/MUmarAmanat/dlt_aws.git into Databricks Repos
2. First execute `StreamDataProducer` Notebook
3. Execute `StreamDataProducer` Notebook from Workflow/Delta Live Tables as DLT pipeline
