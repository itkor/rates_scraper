# Start from python slim-buster docker image, change python version as desired.
FROM python:3.8-buster
RUN apt-get update
# Copy files to working directory
COPY ./rates_parser /app/rates_parser
COPY requirements.txt /app/requirements.txt
WORKDIR /app/rates_parser

# Install your dependencies from requirements.txt
RUN pip install -r /app/requirements.txt

# Run scrapy - the container will exit when this finishes!
CMD scrapy crawl ratescrawler