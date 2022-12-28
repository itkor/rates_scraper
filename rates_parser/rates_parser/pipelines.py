# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# setting up mongo db and collections ===============================
mongo_uri = 'mongodb://localhost:27017/'
mongo_db = 'currency_rates'

client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]
collection_name = 'rates'
loc_collection = db[collection_name]

class RatesParserPipeline:
    collection_name = 'rates'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def process_item(self, item, spider):
        # print(dict(item))
        # TODO: Add exception handling. Notifications of errors
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

