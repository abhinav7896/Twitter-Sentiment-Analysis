import json
import pandas as pd

idf_table = dict()
keywords = ['Canada', 'education', 'university', 'Dalhousie', 'Halifax', 'business']
with open('./rf_table.json', 'r') as file:
    rf_table = json.load(file)
idf_table 
for keyword in keywords:
    df = pd.DataFrame(rf_table[keyword])
    df = df[['Document_no', 'Total_words', 'Term_frequency', 'Relative_frequency']]
    df.to_csv('{}_rel_freq.csv'.format(keyword), index=False)
df    