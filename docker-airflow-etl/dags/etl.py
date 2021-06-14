from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

import boto3
import os

ENV_S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
ENV_S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENV_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ENV_S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ENV_S3_REGION = os.getenv("S3_REGION")
ENV_NODE_ETHEREUM = os.getenv("NODE_ETHEREUM")

default_args = {
    'owner': 'airflow',
    'description': 'Use of the DockerOperator',
    'depend_on_past': False,
    'start_date': datetime(2018, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('docker_dag', default_args=default_args, schedule_interval="5 * * * *", catchup=False) as dag:

    def upload_file(source, target):

        session = boto3.session.Session()
        client = session.client('s3',
                                region_name=ENV_S3_REGION,
                                endpoint_url=ENV_S3_ENDPOINT,
                                aws_access_key_id=ENV_S3_ACCESS_KEY,
                                aws_secret_access_key=ENV_S3_ACCESS_KEY)

        client.upload_file(source,  # Path to local file
                           ENV_S3_BUCKET_NAME,  # Name of Space
                           target)  # Name for remote file

    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )

    t2 = DockerOperator(
        task_id='docker_command',
        image='ethereum-etl:latest',
        api_version='auto',
        auto_remove=True,
        command="export_all -s 0 -e  200 -p %s.".format(ENV_NODE_ETHEREUM),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t3 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello world"'
    )
    t1 >> t2 >> t3
