FROM python:3.8
FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.3.0}

USER root
RUN cd /opt/ \
    && python3 -m venv env \
    && source env/bin/activate \
RUN sudo chmod -R a+rwx /opt/

USER ${AIRFLOW_UID}
COPY ./rates_parser/requirements.txt /requirements.txt
# Install your dependencies from requirements.txt
RUN pip install -r /requirements.txt
RUN pip install scrapy
RUN cd /opt/airflow/
RUN scrapy startproject rates_parser

# Copy files to working directory
COPY ./rates_parser /opt/airflow/rates_parser
WORKDIR /opt/airflow/rates_parser/


ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/rates_parser/"
