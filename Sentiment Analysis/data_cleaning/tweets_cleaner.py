import json
import re

with open('tweets.json', 'r', encoding='ascii', errors='ignore') as json_file:
    tweets_json=json.load(json_file)
    tweets = tweets_json['tweets']

#If for a tweet, retweeted status doesn't exist, the full_text for retweeted status will be "-1"    
default = {"full_text": "-1"}       

#Required tweets which are clean and loadable to MongoDB
tweets_req = dict()

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # most of the emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

#This list stores the full_text of the actual tweet and is free of emojis and other non ascii symbols
tweeted_texts = list(map(lambda x: emoji_pattern.sub(r'', x["full_text"]).encode(encoding='ascii',errors='ignore').decode('ascii'), tweets))

#This list stores the full_text of the retweeted_status and is free of emojis and other non ascii symbols
retweeted_texts = list(map(lambda x: emoji_pattern.sub(r'', x.get("retweeted_status", default)["full_text"]).encode(encoding='ascii',errors='ignore').decode('ascii'), tweets)) 
retweeted_texts = list(filter(lambda x: x != "-1", retweeted_texts))

all_tweet_texts = tweeted_texts + retweeted_texts

all_tweet_docs = [{'text': v} for v in all_tweets]

tweets_req['loadable'] = all_tweet_docs

with open('tweets_clean.json', 'w') as file:
    json.dump(tweets_req, file)
all_tweet_docs

