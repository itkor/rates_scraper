# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import re
import psycopg
from scrapy.exceptions import CloseSpider
from PostgresLogger import PostgresHandler

default_logger = logging.getLogger(__name__)

class PostgresPipeline:
    def __init__(self, hostname, username, password, database, log_database):

        self.instance_name = 'PostgresPipeline'

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.log_database = log_database

        try:
            ## Create/Connect to database
            self.connection = psycopg.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        except Exception as e:
            default_logger.error(f"Error while connecting to Postgres at: {self.hostname}")
            raise CloseSpider(f"Error while connecting to Postgres at: {self.hostname}")

        self.logger = PostgresHandler(hostname,username,password,log_database, self.instance_name)

        ## Create cursor, used to execute commands
        self.cur = self.connection.cursor()

        ## Create quotes table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS public.rates_raw(
            operation_category VARCHAR(25), 
            operation_type VARCHAR(25),
            currency VARCHAR(10),
            currency_description VARCHAR(25),
            rate FLOAT(4), 
            ts timestamp NOT NULL,
            proc_flg BOOLEAN NOT NULL DEFAULT false,
            id SERIAL PRIMARY KEY
        )
        """)
        # PRIMARY KEY(currency,operation_category, ts),
        # CONSTRAINT no_duplicate_tag UNIQUE (currency,operation_category, ts)

        self.connection.commit()
    def preprocess(self, item):
        '''
        Simple cleaning of data
        :param item:
        :return: item
        '''
        try:
            item['rate'] = item['rate'].replace(',','.')
        except Exception as e:
            self.logger.log_error(f"Error while cleaning 'rate' value: {item['rate']}")

        # Leave only digits and dots
        item['rate'] = re.sub("[^0-9.]", "", item['rate'])
        try:
            item['rate'] = float(item['rate'])
        except Exception as e:
            self.logger.log_error(f"Error while converting 'rate' value: {item['rate']}")

        return item

    def process_item(self, item, spider):
        # TODO: Add exception handling. Notifications of errors
        item = self.preprocess(item)

        # self.logger.log_info(f"Item was preprocessed: {item}")

        ## Define insert statement
        self.cur.execute(""" insert into public.rates_raw (operation_category, operation_type, currency, currency_description, rate,ts, proc_flg )
         values (%s,%s,%s,%s,%s,%s, default)""", (
            str(item["operation_category"]),
            str(item["operation_type"]),
            str(item["currency"]),
            str(item["currency_description"]),
            item["rate"],
            item["timestamp"]
        ))

        ## Execute insert of data into database
        self.connection.commit()
        return item
    @classmethod
    def from_crawler(cls, crawler):
        ## pull in information from settings.py
        return cls(
            hostname = crawler.settings.get('PG_HOSTNAME'),
            username = crawler.settings.get('PG_USERNAME'),
            password = crawler.settings.get('PG_PASS'),
            database = crawler.settings.get('PG_DB'),
            log_database = crawler.settings.get('PG_LOGS_DB')
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection to LOGS database
        # self.logger.connect()
        # TODO: Add exception handling. Notifications of errors
        pass

    def close_spider(self, spider):
        # ## Close cursor & connection to LOGS database
        # self.logger.disconnect()

        ## Close cursor & connection to database
        self.cur.close()
        self.connection.close()

