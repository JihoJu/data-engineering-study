from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

# from plugins import slack

import requests
import logging
import psycopg2


""" weather_forcast Table 미리 생성
    DROP TABLE IF EXISTS jihoju96.weather_forecast;
    CREATE TABLE jihoju96.weather_forecast (
        date date primary key,
        temp float,
        min_temp float,
        max_temp float,
        created_date timestamp default GETDATE()
    );
"""


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    link = "{url}&appid={api_key}".format(
        url=context["params"]["url"], api_key=context["params"]["api_key"]
    )
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]

    logging.info(execution_date)
    f = requests.get(link)  # 읽어와야할 api 수가 많다면 멀티스래딩 방식이 좋을 거 같다.

    return f.json()


def transform(**context):
    weather_infos = []
    f_js = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")

    for d in f_js["daily"][1:]:
        weather_infos.append(
            [
                datetime.fromtimestamp(d["dt"]).strftime("%Y-%m-%d"),
                d["temp"]["day"],
                d["temp"]["min"],
                d["temp"]["max"],
            ]
        )
    return weather_infos


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    weather_infos = context["task_instance"].xcom_pull(
        key="return_value", task_ids="transform"
    )
    try:
        sql = "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
        for date, temp, min_temp, max_temp in weather_infos:
            print("날짜:", date, "평균 온도:", temp, "최저 온도:", min_temp, "최고 온도:", max_temp)
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{date}', '{temp}', '{min_temp}', '{max_temp}');"""
        sql += "END;"
        logging.info(sql)
        cur.execute(sql)
    except (Exception) as error:
        print(error)
        cur.execute("ROLLBACK;")


dag_second_assignment = DAG(
    dag_id="second_assignment_weather_api",
    start_date=datetime(2022, 10, 12),
    schedule_interval="0 2 * * *",
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    },
)


extract = PythonOperator(
    task_id="extract",
    python_callable=extract,
    params={
        "url": Variable.get("weather_api_url"),
        "api_key": Variable.get("weather_api_key"),
    },
    dag=dag_second_assignment,
)

transform = PythonOperator(
    task_id="transform", python_callable=transform, params={}, dag=dag_second_assignment
)

load = PythonOperator(
    task_id="load",
    python_callable=load,
    params={"schema": "jihoju96", "table": "weather_forecast"},
    dag=dag_second_assignment,
)

extract >> transform >> load
