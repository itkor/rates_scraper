# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import logging
import re
import psycopg2
from scrapy.exceptions import CloseSpider



# setting up mongo db and collections ===============================
mongo_uri = 'mongodb://localhost:27017/'
mongo_db = 'currency_rates'

client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]
collection_name = 'rates'
loc_collection = db[collection_name]


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

# Setting up logging =============================================
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# info logger
info_logger = setup_logger('info_logger', 'ratescrawler_info.log')
# error  logger
error_logger = setup_logger('error_logger', 'ratescrawler_error.log')
# stat logger
stat_logger = setup_logger('stat_logger', 'ratescrawler_stat.log')

class MongoPipeline:
    collection_name = 'rates'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def preprocess(self, item):
        '''
        Simple cleaning of data
        :param item:
        :return: item
        '''
        try:
            item['rate'] = item['rate'].replace(',','.')
        except Exception as e:
            error_logger.error(f"Error while cleaning 'rate' value: {item['rate']}")

        # Leave only digits and dots
        item['rate'] = re.sub("[^0-9.]", "", item['rate'])
        try:
            item['rate'] = float(item['rate'])
        except Exception as e:
            error_logger.error(f"Error while converting 'rate' value: {item['rate']}")

        return item

    def process_item(self, item, spider):
        # print(dict(item))
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
        ## opening db connection
        # TODO: Add exception handling. Notifications of errors
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        ## clean up when spider is closed
        self.client.close()

class PostgresPipeline:
    def __init__(self, hostname, username, password, database):

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database

        try:
            ## Create/Connect to database
            self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        except Exception as e:
            error_logger.error(f"Error while connecting to Postgres at: {self.hostname}")
            raise CloseSpider(f"Error while connecting to Postgres at: {self.hostname}")

        ## Create cursor, used to execute commands
        self.cur = self.connection.cursor()

        ## Create quotes table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS rates_raw(
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
            error_logger.error(f"Error while cleaning 'rate' value: {item['rate']}")

        # Leave only digits and dots
        item['rate'] = re.sub("[^0-9.]", "", item['rate'])
        try:
            item['rate'] = float(item['rate'])
        except Exception as e:
            error_logger.error(f"Error while converting 'rate' value: {item['rate']}")

        return item

    def process_item(self, item, spider):
        # print(dict(item))
        # TODO: Add exception handling. Notifications of errors
        item = self.preprocess(item)

        ## Define insert statement
        self.cur.execute(""" insert into rates_raw (operation_category, operation_type, currency, currency_description, rate,ts )
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
        )
    def open_spider(self, spider):
        ## initializing spider
        ## opening db connection
        # TODO: Add exception handling. Notifications of errors
        pass

    def close_spider(self, spider):
        ## Close cursor & connection to database
        self.cur.close()
        self.connection.close()