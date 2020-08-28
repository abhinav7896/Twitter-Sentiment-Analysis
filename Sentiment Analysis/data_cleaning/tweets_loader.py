import json
import pymongo
from pymongo import MongoClient

MONGO_DB_URL = "ec2-34-217-114-4.us-west-2.compute.amazonaws.com"

def load_tweets(loadable_tweets):
    client = MongoClient(MONGO_DB_URL, 27017)
    client.list_database_names()
    db = client['sentiment_analysis']
    tweets_collection = db['tweets']
    result = tweets_collection.delete_many({})
    print("\n Deleted {} documents".format(result.deleted_count))
    print("\n Inserting a batch of new documents")
    result = tweets_collection.insert_many(loadable_tweets)
    if(result.inserted_ids != []):
        print("\n Inserted {} documents".format(len(result.inserted_ids)))
        
with open('tweets_clean.json', 'r') as json_file:
    tweets_req=json.load(json_file)
    
load_tweets(tweets_req['loadable'])
tweets_req


