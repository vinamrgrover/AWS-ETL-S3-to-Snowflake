import boto3
from datetime import datetime
import time
from airflow.models import TaskInstance, DagRun
from datetime import datetime
import os

client = boto3.client('emr-serverless', 'ap-south-1')

# Getting the required Environment Variables
application_id = os.getenv('application_id')
execution_role_arn = os.getenv('execution_role_arn')
S3_script_location = os.getenv('S3_script_location')

def start_application():
    response = client.start_application(
        applicationId = application_id
    )

    time.sleep(30)

    return response


def verify_status(): # Verifying the Application's State until its 'STARTED'
    while True:

        response = client.get_application(
            applicationId = application_id
        )['application']['state']

        if response == 'STARTED':
            return response

        time.sleep(15)


def submit_job(**kwargs):
    output = kwargs['ti'].xcom_pull(task_ids = 'get_application_status', key = 'return_value')

    if output == 'STARTED':  # Submitting Job if the application is in its 'STARTED' stage
        response = client.start_job_run(
            applicationId = application_id,
            executionRoleArn = execution_role_arn,
            jobDriver = {
                'sparkSubmit' : {
                    'entryPoint' : S3_script_location
                }
            },
            name = f'Habitation_{datetime.now()}'
        )

        return response


def check_job_run_status(**kwargs):
    output = kwargs['ti'].xcom_pull(task_ids = 'submit_spark_job', key = 'return_value')   # Getting jobRunId from the previous task

    while True:
        response = client.get_job_run(      # getting the Job Run status
            applicationId=application_id,
            jobRunId=output['jobRunId']
        )

        if response['jobRun']['state'] == 'SUCCESS':
            return 'success'

        elif response['jobRun']['state'] == 'FAILED':
            return 'failed'

        time.sleep(30)


def stop_application(**kwargs):
    response = client.stop_application(
        applicationId = application_id
    )

    # Retrieving the DAG object
    execution_date = kwargs['execution_date']
    dag_run = kwargs['dag_run']
    dag = dag_run.dag

    task = dag.get_task('failed') # Getting the task object
    task_instance = TaskInstance(task = task, execution_date = execution_date)

    if task_instance.current_state() == 'success': # if the job run has failed (i.e. Task 'failed' has succeeded), we can skip the 'SnowFlake_Query' Task
        task = dag.get_task('SnowFlake_Refresh')
        task_instance = TaskInstance(task = task, execution_date = execution_date)
        task_instance.set_state('skipped') # Marking the task as skipped
