import json
import pandas as pd

idf_table = dict()
with open('./idf_table.json', 'r') as file:
    idf_table = json.load(file)
idf_table 
df = pd.DataFrame(idf_table)
df2 = df.T[['df', 'total_by_df', 'idf']]
df2.to_csv('idf_table.csv')