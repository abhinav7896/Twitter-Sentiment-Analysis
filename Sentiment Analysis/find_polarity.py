#Discover polarity and findings for tableau

import os
import findspark
findspark.init('/home/ubuntu/server/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pyspark.sql as pssql
import json
import string
from pyspark.sql.functions import monotonically_increasing_id 



def get_match_polarity(row):
    matches = []
    bow = row.Bag_of_Words
    sno = row.tweet_id
    tweet_text = row.text
    for i in pos_words:
        if i in bow.keys():
            matches.append({'tweet_id': sno, 'tweet': tweet_text, 'match': i, 'polarity': 'positive', 'frequency': bow[i]}) 
    for i in neg_words:
        if i in bow.keys():
            matches.append({'tweet_id': sno, 'tweet': tweet_text, 'match': i, 'polarity': 'negative', 'frequency': bow[i]}) 
    if matches == []:
        matches.append({'tweet_id': sno, 'tweet': tweet_text, 'match': 'N/A', 'polarity': 'neutral', 'frequency': 0}) 
    return matches

def matches_polarites():
    matches_polarites = df_bows.rdd.map(get_match_polarity).collect()
    return matches_polarites

global spark, df_tweets, df_bows, pos_words, neg_words 

with open('./positive_words', 'r') as file:
    pos_words = file.read().split('\n')

with open('./negative_words', 'r') as file:
    neg_words = file.read().split('\n')

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sentiment_analysis.tweets")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sentiment_analysis.tweetPolarity") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1')\
    .getOrCreate()

df_tweets = spark.read.format("mongo").load()
df_bows = spark.read.format("mongo").option("uri",
"mongodb://localhost/sentiment_analysis.bagOfWords").load()
df_tweets = df_tweets.select("*").withColumn("tweet_id", monotonically_increasing_id() + 1)
df_tweets.createOrReplaceTempView('tweets')
findings = matches_polarites()
findings_json = []
for i in findings:
    findings_json = findings_json + i
    
with open('findings.json', 'w') as file:
    json.dump(findings_json, file)

findings_json

