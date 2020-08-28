#Create bag of words

import os
import findspark
findspark.init('/home/ubuntu/server/spark-2.4.5-bin-hadoop2.7')
from pyspark.sql import SparkSession
import pyspark.sql as pssql
import json
import string
from pyspark.sql.functions import monotonically_increasing_id 

#-----------------------------------------------------DONE----------------------------------------------------------------
def generate_bow(tweet):
    tweet = tweet.translate({ord(i): None for i in string.punctuation})
    tweet = tweet.lower()
    words = tweet.split()
    bag_of_words = dict()
    for i in words:
        if(i not in bag_of_words.keys()):
            bag_of_words[i] = 1
        else:
            bag_of_words[i] += 1
    return bag_of_words

def get_bows():
    bows = df.rdd.map(lambda x: generate_bow(x.text)).collect()
    return bows

global spark, df, df2
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sentiment_analysis.tweets")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sentiment_analysis.bagOfWords") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.4.1')\
    .getOrCreate()

df = spark.read.format("mongo").load()
df = df.select("*").withColumn("tweet_id", monotonically_increasing_id() + 1)
bows = get_bows()
bows_schema = pssql.types.StructType([pssql.types.StructField(('tweet_id'), pssql.types.IntegerType()),
                                      pssql.types.StructField(('Bag_of_Words'), pssql.types.MapType(pssql.types.StringType(), pssql.types.IntegerType()))])
                                                               
list_bows = [[i+1, j] for i, j in enumerate(bows)]
df2 = spark.createDataFrame(list_bows, schema=bows_schema) 
df3 = df2.join(df, 'tweet_id', 'inner').drop('_id')
df3.write.format("mongo").mode("overwrite").save()
df3.count()



