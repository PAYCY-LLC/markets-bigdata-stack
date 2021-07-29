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
from airflow.utils.task_group import TaskGroup

ENV_S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
ENV_S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
ENV_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
ENV_S3_ENDPOINT = os.getenv("S3_ENDPOINT")
ENV_S3_REGION = os.getenv("S3_REGION")
ENV_NODE_ETHEREUM = os.getenv("NODE_ETHEREUM")
ENV_DATADIR_EXTERNAL = os.getenv("DATADIR_EXTERNAL")

root_path = '/opt/airflow/etl'
default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2018, 1, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=2)
}

with DAG('ethereum_export_dag', schedule_interval='@once', default_args=default_args, catchup=False) as dag:
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
                'start': start,
                'end': end
            },
            dag=dag
        )


    def delete_files_task():
        return PythonOperator(
            task_id='delete_files',
            python_callable=delete_files,
            dag=dag
        )


    def upload_files(start, end):
        files = glob.glob(f'{root_path}/*/start_block={start}/end_block={end}/*.csv')

        for file in files:
            with open(file, 'rb') as data:
                file_key = file[len(root_path) + 1:]

                s3_client.put_object(
                    Bucket=ENV_S3_BUCKET_NAME,
                    Key=file_key,
                    Body=data
                )


    def delete_files():
        dirs = os.listdir(root_path)
        for subdir in dirs:
            dir = os.path.join(root_path, subdir)
            shutil.rmtree(dir, ignore_errors=True)


    batch_from = int(Variable.get(key='eth_export_batch_from', default_var=12_000_000))
    batch_to = int(Variable.get(key='eth_export_batch_to', default_var=12_000_100))
    chunk = int(Variable.get(key='eth_export_chunk', default_var=10))

    with TaskGroup(group_id=f'export_group_from_{batch_from}_to_{batch_to}') as task_group:
        for i in range(batch_from, batch_to, chunk):
            start = i + 1
            end = i + chunk

            start_str = f'{start:08}'
            end_str = f'{start:08}'

            task_export = export_all(start_str, end_str, chunk)
            task_upload = upload_to_s3(start_str, end_str)

            task_export >> task_upload

    task_start >> task_group >> delete_files_task() >> task_end
