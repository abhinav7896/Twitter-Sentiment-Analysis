import json
import pymongo
from pymongo import MongoClient

MONGO_DB_URL = "ec2-54-185-24-247.us-west-2.compute.amazonaws.com"


client = MongoClient(MONGO_DB_URL, 27017)
client.list_database_names()
db = client['sentiment_analysis']
bows = db['bagOfWords']
bows_list = []
cursor = bow.find({})
for doc in cursor:
    bows_list.append(doc)
df = pd.DataFrame(bows_list)
df = df[['tweet_id', 'text', 'Bag_of_Words']]
df.to_csv('bag_of_words.csv')
df