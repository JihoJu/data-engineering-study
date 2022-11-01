from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta

from airflow.exceptions import AirflowException

import requests
import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')   # autocommit = False
    return hook.get_conn().cursor()


def execSQL(**context):
    schema = context['params']['schema']    # schema
    num = context['params']['summary_num']  # 생성할 summary table 개수

    cur = get_Redshift_connection()

    for i in range(num):
        table = context['params']['table'][i]
        selected_sql = context['params']['sql'][i]

        logging.info(schema)
        logging.info(table)
        logging.info(selected_sql)

        sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
        sql += selected_sql
        cur.execute(sql)

        cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError(f"{schema}.{table} didn't have any record")

        try:
            sql = f"""DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};"""
            sql += "COMMIT;"
            logging.info(sql)
            cur.execute(sql)
        except Exception as e:
            cur.execute("ROLLBACK")
            logging.error('Failed to sql. Completed ROLLBACK!')
            raise AirflowException("")


dag = DAG(
    dag_id = "Build_Summary",
    start_date = datetime(2020, 10, 20),
    schedule_interval = '@once',    # 주기적으로 실행 x, 필요 시에만 run
    catchup = False
)

schema = 'jihoju96'
tables = ['channel_summary', 'nps_summary']

""" 실행 sql 정의 """
channel_summary_sql = """SELECT DISTINCT A.userid,
                        FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,
                        LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Last_Channel
                        FROM raw_data.user_session_channel A
                        LEFT JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid;"""

nps_summary_sql = f"""SELECT LEFT(created_at, 10) as date,
                        ROUND(SUM(CASE
                                    WHEN score >= 9 THEN 1
                                    WHEN score <= 6 THEN -1
                                    END)::float*100/COUNT(1), 2)
                        FROM {schema}.nps
                        GROUP BY 1
                        ORDER BY 1;"""

sqls = [channel_summary_sql, nps_summary_sql]

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema': schema,
        'table': tables,
        'sql': sqls,
        'summary_num': 2
    },
    provide_context = True,
    dag = dag
)
