# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
import scrapy
import pymongo
# from bson.objectid import ObjectId
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import os


class GoodreadsScraperPipeline:
    # def __init__(self):
    #     # Connection String
    #     econnect = str(os.environ['Mongo_HOST'])
    #     #self.client = pymongo.MongoClient('mongodb://mymongodb:27017')
    #     self.client = pymongo.MongoClient('mongodb://'+econnect+':27017')
    #     self.db = self.client['dbmycrawler'] #Create Database      
    #     pass
    
    def process_item(self, item, spider):
        
        # collection =self.db['tblunitop'] #Create Collection or Table
        # try:
        #     collection.insert_one(dict(item))
        #     return item
        # except Exception as e:
        #     raise DropItem(f"Error inserting item: {e}")       
        pass
