from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

""" ======================================
이 방법은 첫 번째 실행에는 문제가 없지만 두 번째 실행되면 에러가 발생한다.
=> S3 내 저장하려고 하는 bucket/key 와 같은 파일이 존재한다면 에러!!!
====================================== """

dag = DAG(
    dag_id="MySQL_to_Redshift",
    start_date=datetime(2022, 8, 24),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval="0 9 * * *",  # 매일 9시 0분에 실행
    max_active_runs=1,  # 최대 실행 dag 수
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)

schema = "jihoju96"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

mysql_to_s3_nps = MySQLToS3Operator(
    task_id="mysql_to_s3_nps",
    query="SELECT * FROM prod.nps",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    mysql_conn_id="mysql_conn_id",
    aws_conn_id="aws_conn_id",
    verify=False,
    dag=dag,
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id="s3_to_redshift_nps",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    schema=schema,
    table=table,
    copy_options=["csv"],
    redshift_conn_id="redshift_dev_db",
    aws_conn_id="aws_conn_id",
    method="REPLACE",
    dag=dag,
)

mysql_to_s3_nps >> s3_to_redshift_nps
