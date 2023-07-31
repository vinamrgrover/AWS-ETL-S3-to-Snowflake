# AWS-ETL-S3-to-Snowflake

## Architecture Diagram

![S3_to_Snowflake drawio-3](https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/351a3dce-81fe-49bb-bcaa-c24ec53cca63)

## Description

An ETL Pipeline which processes data from an S3 bucket, performs data transformation using EMR-Serverless, and loads the processed data back into the same S3 bucket. 

The processed data is then made accessible through a SnowFlake External Table, allowing querying of data directly from the S3 bucket. The orchestration of the ETL Pipeline is managed by Apache Airflow.

Appropriate IAM Roles are set up accordingly to ensure a secured workflow.

The Dataset is based on NRDWP (National Rural Drinking Water Programme) of the Indian Government
([Download here](https://data.gov.in/resource/basic-habitation-information-1st-april-2012)).

## Code Overview

***[etl.py](https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/blob/main/etl.py)*** : Contains PySpark Code that will be submitted to EMR-Serverless Application

***[emr_application.py](https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/blob/main/emr_application.py)*** : Contains utility functions to control EMR-Serverless Application

***[dag.py](https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/blob/main/dag.py)*** : Contains Airflow DAG configuration and Tasks.

**Note: store the emr_application.py and dag.py in your DAGs Folder**

## Downloading the Dataset

First, you have to download the Dataset and load it into an S3 Bucket ([Download here](https://data.gov.in/resource/basic-habitation-information-1st-april-2012)).

You can use the AWS CLI and `aws s3 cp <file_source> <s3_url>` command to copy the files from your local machine to S3 Bucket or upload it manually.

<img width="1125" alt="Screenshot 2023-07-31 at 9 47 19 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/e533dee7-6de2-42b1-909e-a3bd5aa1ca61">



## Setting up required resources

## 1. Creating an EMR-Serverless Application

Create an EMR-Serverless Application with the following configurations: 

<img width="831" alt="Screenshot 2023-07-30 at 10 18 23 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/bc0eaca1-9e62-45f9-b28d-e1a9a161a2c9">

<img width="793" alt="Screenshot 2023-07-30 at 10 16 53 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/6f4d19f4-00b0-40fc-a956-3790a8d7aca8">

Leave all the other options as Default. 

### 1.1 Reviewing our Spark Job's Script

Here's our Job's Script : [etl.py](https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/blob/main/etl.py)

The script performs transformation on the original dataset and writes it in Parquet Format.

**(you can change S3 Paths on lines 73 and 80 accordingly)**

***Save the script in S3 Bucket with appropriate path.***

<img width="1146" alt="Screenshot 2023-07-31 at 9 51 23 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/3d84049f-93ce-45d7-9167-4d16ef6a1255">


## 2. Setting up Airflow on EC2-Instance

### 2.1 Creating an EC2 Instance

Spin up an EC2 Instance with an Instance type equal or above "t3.medium". 

Edit the inbound security group rule settings: 

<img width="1347" alt="Screenshot 2023-07-30 at 10 51 22 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/a3d0e67d-593a-490b-9f08-7bed5f5b4af7">

These settings enables us to SSH into the EC2 instance and access Airflow UI on port 8080. 

Leaving other settings as default, Launch the EC2 Instance. 


### 2.2 Creating an EMR-Serverless Execution role

Create an IAM Policy named **EMR-Serverless-Execution-Policy**:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "<S3_Bucket_ARN>/*",
                "<S3_Bucket_ARN>"
            ]
        }
    ]
}
```

Replace **<S3_Bucket_ARN>** with the ARN of your S3 Bucket.

On the create IAM Role Page, select ***custom trust policy*** and add the following trust policy:

```
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "emr-serverless.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```



Attach the **EMR-Serverless-Execution-Policy** to the IAM Role. Name your role **EMR-Serverless-Execution-Role**.


### 2.3 Creating an IAM Role for EC2 Instance

Create an IAM Policy for the EC2 Instance to access EMR-Serverless

```
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": "emr-serverless:*",
			"Resource": [
				"<emr_serverless_application_arn>",
				"<emr_serverless_application_arn>/jobruns/*"
			]
		},
		{
			"Effect": "Allow",
			"Action": "iam:PassRole",
			"Resource": "<emr-serverless-execution-role-arn>"
		}
	]
}
```


Replace the **emr_serverless_application_arn** with the ARN of your previously created EMR-Serverless Application

**Attach the following IAM Role to your EC2 Instance**

### 2.4 Installing Airflow

SSH into your EC2 Instance and Install Python and Airflow on your EC2 Instance with the appropriate dependencies. You can easily get a guide on how to do so.

Execute the following shell command on your EC2 Instance:

`airflow init`

This command will initialize an Airflow Database.

Next, Execute the following command to create an Airflow user:

```
airflow create_user \
--email email --firstname firstname \
--lastname lastname --password password \
--role Admin --username username
```
Replace email, firstname, lastname, password, and username with appropriate options. Select the role as Admin.

Now, execute the following command

`airflow webserver -p 8080` 

This command starts airflow webserver on port 8080.

Airflow UI can now be accessed on EC2 Instance's Public IP Address on Port 8080.

`http://<Instance's public IP>:8080`

Replace `<Instance's public IP>` with the Public IP Address of EC2 Instance. 


<img width="1440" alt="Screenshot 2023-07-31 at 12 33 03 AM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/69663880-3439-46bc-8b79-09db0c9c9601">


Verify by entering your username and password.

## 3. Integrating S3 with Snowflake

### 3.1 Creating a Snowflake IAM Role

Create an IAM Policy named **snowflake_access**:

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "statement1",
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "<S3_Bucket_ARN>/*"
        },
        {
            "Sid": "statement2",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "<S3_Bucket_ARN>"
        }
    ]
}
```

Replace `<S3_Bucket_ARN>` with the ARN of your S3 Bucket. 

Now, create an IAM Role named **snowflake_role** with the previously created **snowflake_access** IAM Policy attached to it.


### 3.2 Creating Snowflake Storage Integration

Execute the following command on Snowflake Console to create a Storage Integration:


```
CREATE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<snowflake_role_arn>'
STORAGE_ALLOWED_LOCATIONS = ('s3://<your_bucket_name>');
```

Replace **<snowflake_role_arn>** with the ARN of the previously created **snowflake_role**

Also Replace **<your_bucket_name>** with the name of your S3 Bucket.

Now execute the following command on Snowflake Console:

`DESC INTEGRATION s3_int;`

Copy values of the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.

### 3.3 Editing snowflake_role

Navigate to the previously created **snowflake_role** on the IAM Console and edit the ***Trust relationships*** for the Role.

Add the following trust policy: 

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
                }
            }
        }
    ]
}
```

Replace `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` with the values retrieved from the last step.


### 3.4 Creating Stage

Execute the following command to create an External Stage in Snowflake:

```
CREATE STAGE s3_stage
URL = 's3://<your_bucket_name>/'
STORAGE_INTEGRATION = s3_int;
```

Replace **<your_bucket_name>** with the name of your S3 Bucket.

Finally, verify the integration by executing the following command:

`LIST @S3_STAGE;`

If the command returns the objects in your Bucket, the integration is successful.


## 3.5 Creating External Table

Create a File format:

```
CREATE FILE FORMAT PARQUET_FORMAT
TYPE = PARQUET
COMPRESSION = SNAPPY;
```

Grant necessary privileges:

```
GRANT CREATE STAGE ON SCHEMA public TO ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION s3_int TO ROLE ACCOUNTADMIN;
```
These privileges will allow the ACCOUNTADMIN role to create stage and access the Integration created previously.

Execute the following command to create an External Table in Snowflake:


```
CREATE OR REPLACE EXTERNAL TABLE habitation (
  state_name VARCHAR AS SPLIT_PART(
        SPLIT_PART(METADATA$FILENAME, '/', 3),
        'state_name=', 2),
  district_name VARCHAR AS (VALUE:district_name::VARCHAR),
  block_name VARCHAR AS (VALUE:block_name::VARCHAR),
  panchayat_name VARCHAR AS (VALUE:panchayat_name::VARCHAR),
  village_name VARCHAR AS (VALUE:village_name::VARCHAR),
  habitation_name VARCHAR AS (VALUE:habitation_name::VARCHAR),
  sc_current_population BIGINT AS (VALUE:sc_current_population::BIGINT),
  st_current_population BIGINT AS (VALUE:st_current_population::BIGINT),
  general_current_population BIGINT AS (VALUE:general_current_population::BIGINT),
  sc_covered_population BIGINT AS (VALUE:sc_covered_population::BIGINT),
  st_covered_population BIGINT AS (VALUE:st_covered_population::BIGINT),
  general_covered_population BIGINT AS (VALUE:general_covered_population::BIGINT),
  status VARCHAR AS (VALUE:status::VARCHAR)
)
PARTITION BY (
  state_name
)
LOCATION = @S3_STAGE/transformed/habitation 
FILE_FORMAT = PARQUET_FORMAT;
```

**Modify the location accordingly with Path to your transformed Parquet files in S3**

## 4. Integrating Snowflake with Airflow

### 4.1 Installing Snowflake provider

Execute the following command on your EC2 Instance:

```pip install apache-airflow-providers-snowflake``` 

This command will install Snowflake provider for Apache-Airflow.

### 4.2 Adding Connection

Navigate to the Airflow UI which we opened previously.

Hover on the ***Admin*** Tab and select ***Connections***. Click on the **+** Sign to add a new Connection.

Fill in the following details accordingly:

<img width="1432" alt="Screenshot 2023-07-31 at 6 27 10 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/d73cef70-cb1a-4cb0-bfec-770233c2cf6d">


<img width="1428" alt="Screenshot 2023-07-31 at 6 22 42 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/3a623d25-4b03-4341-b594-ecde24614cf7">

Replace the following:

**Schema** : Snowflake Schema

**Login** : Snowflake username

**Password** Snowflake password

**Account** : Snowflake account URL

**Warehouse** : Snowflake Warehouse

**Database** : Snowflake Database

**Region** : AWS Region

**Role** : Snowflake Role

Click on **Test** to test your connection. 

You should recieve a message like this:

<img width="1432" alt="Screenshot 2023-07-31 at 6 23 30 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/37190537-a51f-46ad-bca4-b51a1f048d66">


## 5. Reviewing DAG

### 5.1 Setting up essential environment variables

First execute the following commands on your EC2 Instance:

```
export application_id=<emr_serverless_application_id>
export execution_role_arn=<emr_serverless_execution_role_arn>
export S3_script_location=<S3_location_job_script>
```

Replace:

<emr_serverless_application_id> with the ARN of the EMR-Serverless Application **(Created in step 1)**

<emr_serverless_execution_role_arn> with the ARN of the **EMR-Serverless-Execution-Role** **(Created in step 2.1)**

<S3_location_job_script> with the S3 URL of the Job's Script **(Saved in step 1.1)**

### 5.2 Triggering DAG

Run the following command to start Airflow webserver and scheduler:

`airflow webserver -p 8080 & airflow scheduler &`

This command starts Airflow Webserver and Scheduler as Daemons, which allows us to execute commands in the shell without freezing it.

Navigate to the Airflow UI, Click on the DAG Named ***"S3_to_Snowflake"***

Select the Play button on the upper right and click ***"Trigger DAG"***

Click the ***Graph*** tab to view the DAG's Graph

### 5.3 Two states of our DAG

#### Failed

If our Job Run fails, then the last task, i.e., ***SnowFlake Refresh***, won't be executed:

<img width="1422" alt="Screenshot 2023-07-31 at 7 30 12 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/759c3277-3ff7-46fb-9ef5-2a281050a84b">


As you can see our last Task (SnowFlake Refresh) has been marked as Skipped

#### Success

If our Job Run is Succeeded, all the tasks will be executed normally:

<img width="1419" alt="Screenshot 2023-07-31 at 7 40 32 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/83c3a38e-9d1a-4d35-ac99-96206ccc1b0d">

You can even check if the Job Run has succeeded by executing the following command in Snowflake Console:


## Wrapping up

Here are our Processed Parquet Files:

<img width="1092" alt="Screenshot 2023-07-31 at 10 20 51 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/9ca5cb55-5f31-4492-a54d-b29b6cb3a9b4">


You can check if the External Table has been refreshed by executing the following command in Snowflake Console:

```
SELECT
    DISTINCT state_name
FROM habitation;
```

<img width="945" alt="Screenshot 2023-07-31 at 10 13 41 PM" src="https://github.com/vinamrgrover/AWS-ETL-S3-to-Snowflake/assets/100070155/1f711d60-0bd7-4c41-82a5-4099e8b6241b">

### Stop Airflow Daemons

You can terminate all the processes associated with airflow by executing the following command on your EC2 Instance:

`ps aux | grep airflow | grep -v grep | awk '{print $2}' | xargs kill`
