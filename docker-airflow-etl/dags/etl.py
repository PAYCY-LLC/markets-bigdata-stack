from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from pathlib import Path
import boto3
import os
import glob

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

    # ----------------------------- Upload dir  -------------------------------------------
    def upload_files(path):

        s3config = {
            "region_name": ENV_S3_REGION,
            "endpoint_url": "https://{}.digitaloceanspaces.com".format(ENV_S3_REGION),
            "aws_access_key_id": ENV_S3_ACCESS_KEY,
            "aws_secret_access_key": ENV_S3_SECRET_KEY }

        s3resource = boto3.resource("s3", **s3config)
        bucket = s3resource.Bucket(ENV_S3_BUCKET_NAME)

        for subdir, dirs, files in os.walk(path):
            for file in files:
                print(f'subdir ------------  {subdir}')
                full_path = os.path.join(subdir, file)
                print(f'full_path ------------  {full_path}')
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=full_path[len(path):], Body=data)


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
        op_kwargs={'path': f'/opt/airflow/etl/'},
        dag=dag
    )

    start_task >> etl_export >> upload_to_s3
