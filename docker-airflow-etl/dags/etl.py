import os
import shutil
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator

ENV_S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
ENV_S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENV_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ENV_S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ENV_S3_REGION = os.getenv("S3_REGION")
ENV_NODE_ETHEREUM = os.getenv("NODE_ETHEREUM")
ENV_DATADIR_EXTERNAL = os.getenv("DATADIR_EXTERNAL")

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
    s3_client = boto3.client(
        's3',
        region_name=ENV_S3_REGION,
        endpoint_url=f'https://{ENV_S3_REGION}.digitaloceanspaces.com',
        aws_access_key_id=ENV_S3_ACCESS_KEY,
        aws_secret_access_key=ENV_S3_SECRET_KEY
    )


    # ----------------------------- Upload dir  -------------------------------------------
    def upload_files(path, start, end):
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)

                with open(file_path, 'rb') as data:
                    dir_parent = os.path.join(root, '../..')
                    dir_current = os.path.dirname(root)

                    s3_client.put_object(
                        Bucket=ENV_S3_BUCKET_NAME,
                        Key=file_path[len(path) + 1:],
                        Body=data
                    )
                    os.remove(file_path)

                    if start == dir_current[-len(start):]:
                        shutil.rmtree(dir_parent, ignore_errors=True)
                    if end == dir_current[-len(end):]:
                        shutil.rmtree(dir_parent, ignore_errors=True)


    # ----------------------------- Tasks -------------------------------------------

    start_task = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )

    etl_export = DockerOperator(
        task_id='docker_command',
        image='blockchainetl/ethereum-etl:latest',
        api_version='auto',
        auto_remove=True,
        volumes=[f'{ENV_DATADIR_EXTERNAL}/etl:/ethereum-etl/output'],
        command=f'export_all -s 1000000 -e 1001000 -b 1000 -p {ENV_NODE_ETHEREUM}',
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_files,
        op_kwargs={
            'path': '/opt/airflow/etl/',
            'start': '1000000',
            'end': '1001000'
        },
        dag=dag
    )

    start_task >> etl_export >> upload_to_s3
