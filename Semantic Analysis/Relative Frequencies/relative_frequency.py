from pyspark.sql import SparkSession
import json
import math
import string


def get_total_words(doc):
    news_title = doc.title.translate({ord(i): None for i in string.punctuation})
    news_content = doc.content.translate({ord(i): None for i in string.punctuation})
    news_desc = doc.description.translate({ord(i): None for i in string.punctuation})
    total_words = len(news_title.split() + news_content.split() + news_desc.split())
    return total_words

def get_d_tfs(keyword):
    d_tfs = news_docs.rdd.map(lambda x: x.title.count(keyword) + x.content.count(keyword) + x.description.count(keyword)).collect()
    return d_tfs

#doc_total_words -> list of total number of words in each doc    
global spark, news_docs , d_tfs, keywords, doc_total_words        
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://localhost/sentiment_analysis.news") \
    .getOrCreate()

news_docs = spark.read.format("mongo").load()
news_docs = news_docs.na.drop()
news_docs.printSchema()
doc_total_words = news_docs.rdd.map(lambda x: get_total_words(x)).collect()
keywords = ['Canada', 'education', 'university', 'Dalhousie', 'Halifax', 'business']

# Document-term frequencies: term(keyword) frequencies by documents
d_tfs = dict()  

# Relative frequency table      
rf_table = dict()     

for keyword in keywords:
    d_tfs[keyword] = get_d_tfs(keyword)    #List of frequencies for each keyword wrt documents
    rf_table[keyword] = [{'Document_no': i+1, 'Total_words': doc_total_words[i], 'Term_frequency': d_tfs[keyword][i], 'Relative_frequency': round(d_tfs[keyword][i]/doc_total_words[i], 3)} 
    for i in range(news_docs.count())]        
    

print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")
print(len(doc_total_words), end='\n')
print(rf_table['Canada'])
print("\n-----------------------------------------------------------\n-----------------------------------------------------------")

with open('./rf_table.json', 'w') as file:
    json.dump(rf_table, file)




