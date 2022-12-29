# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import logging
import re

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

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

class RatesParserPipeline:
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

