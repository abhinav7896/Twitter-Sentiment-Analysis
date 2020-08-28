from pyspark.sql import SparkSession
import json
import math

def get_tfs(keyword):
    tfs = news_docs.rdd.map(lambda x: x.title.count(keyword) + x.content.count(keyword) + x.description.count(keyword)).collect()
    return tfs

def get_hits(keyword):
    tfs = tf[keyword]
    hits = [count for count in tfs if count != 0]
    return hits
    
def get_ratio(keyword):
    if(df[keyword] == 0):
        return 'NA'
    else:
        return total_docs/df[keyword]
        
def get_idf(keyword):
    if(df[keyword] == 0):
        return 'NA'
    else:
        return round(math.log(total_by_df[keyword], 2), 3)
    
global spark, news_docs , tf, df, total_by_df, total_docs, idf, keywords
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost/sentiment_analysis.news") \
    .getOrCreate()

news_docs = spark.read.format("mongo").load()
news_docs = news_docs.na.drop()
news_docs.printSchema()
total_docs = news_docs.count()
keywords = ['Canada', 'education', 'university', 'Dalhousie', 'Halifax', 'business']

tf = dict()
df = dict()
total_by_df = dict()
idf = dict()
idf_table = dict()

for keyword in keywords:
    tf[keyword] = get_tfs(keyword)
    df[keyword] = len(get_hits(keyword))
    total_by_df[keyword] = get_ratio(keyword)
    idf[keyword] = get_idf(keyword)
    idf_table[keyword] = {'total_docs': total_docs, 'df': df[keyword], 'total_by_df': total_by_df[keyword], 'idf': idf[keyword]}        
    
print(total_docs)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(tf)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(df)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(total_by_df)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(idf)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(idf_table)
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")



with open('idf_table.json', 'w') as file:
    json.dump(idf_table, file)




