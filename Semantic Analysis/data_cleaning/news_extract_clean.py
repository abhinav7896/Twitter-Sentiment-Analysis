
import json
from newsapi import NewsApiClient

news_json = dict()
news_list = []
def get_news(keyword):
    newsapi = NewsApiClient(api_key='8ffa0f48ceaa4756af81c5907d15be0f')
    all_articles = newsapi.get_everything(q=keyword,
                                      language='en',
                                      sort_by='relevancy')['articles']
    return all_articles
    
news_list.extend(get_news('Canada'))
news_list.extend(get_news('University”,'))
news_list.extend(get_news('Dalhousie University'))
news_list.extend(get_news('Halfax'))
news_list.extend(get_news('Canada Education'))
news_list.extend(get_news('Moncton'))
news_list.extend(get_news('Toronto'))
news_json['news'] = news_list
#Removes non ascii characters while writing into the file
with open('news_clean.json', 'w', encoding='ascii', errors='ignore') as file:
    json.dump(news_json, file)
news_json


