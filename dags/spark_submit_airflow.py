from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
import airflow.utils.dates

# Configurations
RAW_BUCKET_NAME = "raw-data-bootcamp"
STAGING_BUCKET_NAME = "staging-data-bootcamp"
PRODUCTION_BUCKET_NAME = "production-data-bootcamp"
SCRIPTS_BUCKET_NAME="spark-script-bootcamp"

movies_data = "./dags/data/movie_review.csv"
user_purchase_data = "./dags/data/user_purchase.csv"

movies_review_script = "./dags/scripts/spark/movies_review_spark.py"
user_purchase_script = "./dags/scripts/spark/user_purchase.py"
user_behavior_metrics_script = "./dags/scripts/spark/user_behavior_metrics.py"

s3_movies_data = "movie_review.csv"
s3_user_purchase_data = "user_purchase.csv"

s3_movies_review_script = "movies_review_spark.py"
s3_user_purchase_script = "user_purchase.py"
s3_user_behavior_metrics_script = "user_behavior_metrics.py"


s3_clean = "clean_data/"

# helper function
def _local_to_s3(filename, key, bucket_name):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)

SPARK_STEPS = [
    {
        "Name": "Classify movie reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.SCRIPTS_BUCKET_NAME }}/{{ params.s3_movies_review_script}}",
            ],
        },
    },
    {
        "Name": "User purchase",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--jars",
                "s3://spark-jars-bootcamp/postgresql-42.2.18.jar",
                "s3://{{ params.SCRIPTS_BUCKET_NAME }}/{{ params.s3_user_purchase_script}}",
            ],
        },
    },
    {
        "Name": "User Behavior",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.SCRIPTS_BUCKET_NAME }}/{{ params.s3_user_behavior_metrics_script}}",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "Movie review and User Purchase",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

default_args = {
    "owner": "faider.florez",
    "depends_on_past": False,
    #"wait_for_downstream": True,
    'start_date': airflow.utils.dates.days_ago(1),
    #"email": ["airflow@airflow.com"],
    # "email_on_failure": False,
    #"email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    schedule_interval = '@daily',
    max_active_runs=3
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

#movies_to_s3 = PythonOperator(
#    dag=dag,
#    task_id="movies_to_s3",
#    python_callable=_local_to_s3,
#    op_kwargs={"filename": movies_data, "bucket_name": RAW_BUCKET_NAME, "key": s3_movies_data},
#)

user_purchase_to_s3 = PythonOperator(
    dag=dag,
    task_id="user_purchase_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": user_purchase_data, "bucket_name": RAW_BUCKET_NAME, "key": s3_user_purchase_data},
)


movies_review_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="movies_review_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": movies_review_script, "bucket_name": SCRIPTS_BUCKET_NAME, "key": s3_movies_review_script},
)

user_purchase_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="user_purchase_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": user_purchase_script, "bucket_name": SCRIPTS_BUCKET_NAME, "key": s3_user_purchase_script},
)

user_behavior_metrics_script_to_s3 = PythonOperator(
    dag=dag,
    task_id="user_behavior_metrics_script_to_s3",
    python_callable=_local_to_s3,
    op_kwargs={"filename": user_behavior_metrics_script, "bucket_name": SCRIPTS_BUCKET_NAME, "key": s3_user_behavior_metrics_script},
)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    region_name="us-east-1",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "SCRIPTS_BUCKET_NAME": SCRIPTS_BUCKET_NAME,
        "s3_movies_data": s3_movies_data,
        "s3_movies_review_script": s3_movies_review_script,
        "s3_user_purchase_script":s3_user_purchase_script,
        "s3_user_behavior_metrics_script":s3_user_behavior_metrics_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    region_name="us-east-1",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)


start_data_pipeline >> [user_purchase_to_s3, movies_review_script_to_s3,user_behavior_metrics_script_to_s3,user_purchase_script_to_s3] >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
