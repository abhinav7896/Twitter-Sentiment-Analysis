import json
import pymongo
from pymongo import MongoClient

MONGO_DB_URL = "ec2-34-217-114-4.us-west-2.compute.amazonaws.com"


def load_news(news):
    client = MongoClient(MONGO_DB_URL, 27017)
    client.list_database_names()
    db = client['sentiment_analysis']
    news_collection = db['news']
    result = news_collection.delete_many({})
    print("\n Deleted {} documents".format(result.deleted_count))   
    print("\n Inserting a batch of new documents")
    result = news_collection.insert_many(news)
    if(result.inserted_ids != []):
        print("\n Inserted {}".format(len(result.inserted_ids)))

        
with open('news_clean.json', 'r', encoding='ascii', errors='ignore') as json_file:
    news_json=json.load(json_file)
    news = news_json['news']
    
load_news(news)

