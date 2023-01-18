from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
import os

from datetime import datetime
from datetime import timedelta

MAIN_PG_HOSTNAME = os.environ.get('MAIN_PG_HOSTNAME')
MAIN_PG_USER = os.environ.get('MAIN_PG_USER')
MAIN_PG_PASSWORD = os.environ.get('MAIN_PG_PASSWORD')
MAIN_PG_DB = os.environ.get('MAIN_PG_DB')
MAIN_PG_LOGS_DB = os.environ.get('MAIN_PG_LOGS_DB')

with DAG(
        dag_id='rates_parser_dag',
        start_date=datetime(2023, 1, 10, 1, 13, 0, 0),
        catchup=False,
        schedule_interval=timedelta(hours=4)
) as dag:
    scrapy_parser_task = DockerOperator(
        task_id='scrapy_crawl',
        image='scraper:v1',
        container_name='rates_crawler',
        environment={'MAIN_PG_HOSTNAME': MAIN_PG_HOSTNAME, 'MAIN_PG_USER': MAIN_PG_USER,
                     'MAIN_PG_PASSWORD': MAIN_PG_PASSWORD, 'MAIN_PG_DB': MAIN_PG_DB,
                     'MAIN_PG_LOGS_DB': MAIN_PG_LOGS_DB},
        api_version='auto',
        auto_remove=True,
        command="scrapy crawl ratescrawler",
        docker_url="unix://var/run/docker.sock",
        network_mode="rates_scraper_postgres_main"
    )

scrapy_parser_task