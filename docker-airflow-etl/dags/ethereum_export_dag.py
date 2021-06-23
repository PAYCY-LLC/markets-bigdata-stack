import glob
import os
import shutil
import boto3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
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
    'depend_on_past': False,
    'start_date': datetime(2018, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
}

with DAG('ethereum_export_dag', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    s3_client = boto3.client(
        's3',
        region_name=ENV_S3_REGION,
        endpoint_url=f'https://{ENV_S3_REGION}.digitaloceanspaces.com',
        aws_access_key_id=ENV_S3_ACCESS_KEY,
        aws_secret_access_key=ENV_S3_SECRET_KEY
    )

    task_start = DummyOperator(
        task_id='start',
        dag=dag
    )

    task_end = DummyOperator(
        task_id='end',
        dag=dag
    )


    def export_all(start, end, batch):
        return DockerOperator(
            task_id=f'export_from_{start}_to_{end}',
            image='blockchainetl/ethereum-etl:latest',
            api_version='auto',
            auto_remove=True,
            volumes=[f'{ENV_DATADIR_EXTERNAL}/etl:/ethereum-etl/output'],
            command=f'export_all -s {start} -e {end} -b {batch} -p {ENV_NODE_ETHEREUM}',
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge"
        )


    def upload_to_s3(start, end):
        return PythonOperator(
            task_id=f's3_upload_from_{start}_to_{end}',
            python_callable=upload_files,
            op_kwargs={
                'path': '/opt/airflow/etl',
                'start': start,
                'end': end
            },
            dag=dag
        )


    def upload_files(path, start, end):
        files = glob.glob(f'{path}/*/start_block={start}/end_block={end}/*.csv')

        for file in files:
            with open(file, 'rb') as data:
                file_key = file[len(path) + 1:]
                file_dirs = file.split('/')[:3]
                path_root_dir = os.path.join(*file_dirs)

                s3_client.put_object(
                    Bucket=ENV_S3_BUCKET_NAME,
                    Key=file_key,
                    Body=data
                )
                os.remove(file)
                shutil.rmtree(path_root_dir, ignore_errors=True)


    batch_from = int(Variable.get(key='eth_export_batch_from', default_var=12_000_000))
    batch_to = int(Variable.get(key='eth_export_batch_to', default_var=12_000_100))
    chunk = int(Variable.get(key='eth_export_chunk', default_var=10))

    for i in range(batch_from, batch_to, chunk):
        start = i + 1
        end = i + chunk

        task_export = export_all(start, end, chunk)
        task_upload = upload_to_s3(start, end)

        task_start >> task_export >> task_upload >> task_end
