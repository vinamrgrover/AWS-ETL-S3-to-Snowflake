from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import emr_application
import time
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner' : 'vinamr',
    'start_date' : datetime.utcnow()
}

dag = DAG(
    'S3_to_Snowflake',
    default_args = default_args,
    max_active_runs = 1
)



with dag:

    start_application = PythonOperator(
        task_id = 'start_application',
        python_callable = emr_application.start_application
    )

    get_status = PythonOperator(
        task_id = 'get_application_status',
        python_callable = emr_application.verify_status
    )

    submit_job = PythonOperator(
        task_id = 'submit_spark_job',
        python_callable = emr_application.submit_job
    )

    stop_application = PythonOperator(
        task_id = 'stop_application',
        python_callable = emr_application.stop_application,
        trigger_rule = TriggerRule.ALL_DONE
    )

    check_job_run = BranchPythonOperator(
        task_id = 'check_job_run_status',
        python_callable = emr_application.check_job_run_status
    )

    success = EmptyOperator(
        task_id = 'success'
    )

    failed = EmptyOperator(
        task_id = 'failed'
    )

    load_data = SnowflakeOperator(
        snowflake_conn_id = 'snowflake_connection',
        sql = """
            ALTER EXTERNAL TABLE habitation REFRESH
        """,
        task_id = 'SnowFlake_Refresh',
        trigger_rule = TriggerRule.NONE_FAILED
    )



start_application >> get_status >> submit_job >> check_job_run
check_job_run >> success >> stop_application >> load_data
check_job_run >> failed >> stop_application

