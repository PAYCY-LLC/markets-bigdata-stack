from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator

import boto3
import os

ENV_S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
ENV_S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENV_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ENV_S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ENV_S3_REGION = os.getenv("S3_REGION")
ENV_NODE_ETHEREUM = os.getenv("NODE_ETHEREUM")
ENV_DATADIR_EXTERNAL = os.getenv("DATADIR_EXTERNAL")

s3_conn_id = 's3-conn'
state = 'wa'
date = '{{ yesterday_ds_nodash }}'


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
                                aws_secret_access_key=ENV_S3_SECRET_KEY)

        client.upload_file(source,  # Path to local file
                           ENV_S3_BUCKET_NAME,  # Name of Space
                           target)  # Name for remote file

    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )

    t2 = DockerOperator(
        task_id='docker_command',
        image='blockchainetl/ethereum-etl:latest',
        api_version='auto',
        auto_remove=True,
        volumes=[f'{ENV_DATADIR_EXTERNAL}/etl:/ethereum-etl/output'],
        command=f'export_blocks_and_transactions -s 0 -e 100 --blocks-output /ethereum-etl/output/blocks.csv --transactions-output /ethereum-etl/output/transactions.csv -p {ENV_NODE_ETHEREUM}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    t3 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello world"'
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_file,
        op_kwargs={'source': f'/opt/airflow/etl/blocks.csv',
                   'target': 'blocks/blocks.csv'},
        dag=dag
    )

    t1 >> t2 >> t3 >> upload_to_s3
