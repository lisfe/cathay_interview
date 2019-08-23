# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import logging

class SaveToMongoPipeline(object):
    collection_name = 'objects'
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler.settings.get('MONGO_HOST', 'localhos1'),
            crawler.settings.get('MONGO_PORT', 27017),
            crawler.settings.get('MONGO_DB_NAME', 'rent591db')
        )

    def __init__(self, host, port, db_name):
        self.host = host
        self.port = port
        self.db_name = db_name

    def open_spider(self, spider):
        self.mongo_client = pymongo.MongoClient(self.host, self.port)
        self.db = self.mongo_client[self.db_name]
        self.collection = self.db[self.collection_name]
        # ensure index
        self.collection.create_index([('house_id', pymongo.ASCENDING)], unique=True)
        self.collection.create_index([('$**', pymongo.TEXT)])



    def close_spider(self, spider):
        self.mongo_client.close()

    def process_item(self, item, spider):
        try:
            self.collection.insert_one(dict(item))
        except Exception as e:
            logging.debug(e)

        return item
