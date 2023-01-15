from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime
from datetime import timedelta

with DAG(
        dag_id='rates_parser_dag',
        start_date=datetime(2023, 1, 10, 1, 13, 0, 0),
        catchup=False,
        schedule_interval=timedelta(hours=4)
) as dag:
    scrapy_parser_task = BashOperator(
            task_id='scrapy',
            bash_command='source /opt/env/bin/activate && cd /opt/airflow/rates_parser/ && scrapy crawl ratescrawler')


scrapy_parser_task