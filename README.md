# AWS-ETL-S3-to-Snowflake

## Description

An ETL Pipeline which processes data from an S3 bucket, performs data transformation using EMR-Serverless, and loads the processed data back into the same S3 bucket. 

The processed data is then made accessible through a SnowFlake External Table, allowing querying of data directly from the S3 bucket. The orchestration of the ETL Pipeline is managed by Apache Airflow.

Appropriate IAM Roles are set up accordingly to ensure a secured workflow.

The Dataset is based on NRDWP (National Rural Drinking Water Programme) of the Indian Government
([Download here](https://data.gov.in/resource/basic-habitation-information-1st-april-2012)).




## Downloading the Dataset

First, you have to download the Dataset and load it into an S3 Bucket ([Download here](https://data.gov.in/resource/basic-habitation-information-1st-april-2012)).

You can use the AWS CLI and `aws s3 cp <file_source> <s3_url>` command to copy the files from your local machine to S3 Bucket.

## Setting up required resources

### Creating an EMR-Serverless Application

Create an EMR-Serverless Application with the following configurations: 

<img width="831" alt="Screenshot 2023-07-30 at 10 18 23 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/bc0eaca1-9e62-45f9-b28d-e1a9a161a2c9">

<img width="793" alt="Screenshot 2023-07-30 at 10 16 53 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/6f4d19f4-00b0-40fc-a956-3790a8d7aca8">

Leave all the other options as Default. 



