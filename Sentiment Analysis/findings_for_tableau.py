import json
import pandas as pd

idf_table = dict()
keywords = ['Canada', 'education', 'university', 'Dalhousie', 'Halifax', 'business']
with open('./findings.json', 'r') as file:
    findings_table = json.load(file)
findings_table 
df = pd.DataFrame(findings_table)
df = df[['tweet_id', 'tweet', 'match', 'polarity', 'frequency']]
df.to_csv('findings_tableau.csv')
df
    