FROM python:3.8

USER root

COPY ./rates_parser/requirements.txt /requirements.txt
# Install dependencies from requirements.txt
RUN pip install -r /requirements.txt

RUN cd /opt/
RUN scrapy startproject rates_parser

# Copy files to working directory
COPY ./rates_parser /opt/rates_parser
WORKDIR /opt/rates_parser/
VOLUME  /opt/rates_parser/

ENV PYTHONPATH "${PYTHONPATH}:/opt/rates_parser/"
