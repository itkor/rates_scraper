# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime

import pymongo
import logging
import re
import psycopg as psycopg2
from scrapy.exceptions import CloseSpider
from PostgresLogger import PostgresHandler

default_logger = logging.getLogger(__name__)
class MongoPipeline:

    # setting up mongo db and collections ===============================
    mongo_uri = 'mongodb://localhost:27017/'
    mongo_db = 'currency_rates'

    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db]
    collection_name = 'rates'
    loc_collection = db[collection_name]

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

        self.logger = PostgresHandler()

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
        self.db[self.collection_name].insert_one(dict(item))
        return item


    @classmethod
    def from_crawler(cls, crawler):
        ## pull in information from settings.py
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection to LOGS database
        # self.logger.connect()

        ## opening db connection
        # TODO: Add exception handling. Notifications of errors
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        ## clean up when spider is closed
        self.client.close()

        ## Close cursor & connection to LOGS database
        # self.logger.disconnect()

class PostgresPipeline:
    def __init__(self, hostname, username, password, database, log_database):

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.log_database = log_database

        try:
            ## Create/Connect to database
            self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        except Exception as e:
            default_logger.error(f"Error while connecting to Postgres at: {self.hostname}")
            raise CloseSpider(f"Error while connecting to Postgres at: {self.hostname}")

        self.logger = PostgresHandler(hostname,username,password,log_database, self.connection)

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
            ts timestamp,
            id SERIAL PRIMARY KEY
        )
        """)
        # PRIMARY KEY(currency,operation_category, ts),
        # CONSTRAINT no_duplicate_tag UNIQUE (currency,operation_category, ts)
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

        self.logger.log_info(f"Item was preprocessed: {item}")

        ## Define insert statement
        self.cur.execute(""" insert into public.rates_raw (operation_category, operation_type, currency, currency_description, rate,ts )
         values (%s,%s,%s,%s,%s,%s)""", (
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

