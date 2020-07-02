import sqlite3
import pprint
import pandas as pd
import csv

# con = sqlite3.connect('5chScrape.db')

# df = pd.read_sql_query('select * from sqlite_master where type="table"', con)
# print(df)

# threads = pd.read_sql_query('SELECT * FROM threads ORDER BY title', con)
# print(threads['url'])

# df = pd.read_sql_query('SELECT * FROM {} ORDER BY user'.format(threads['id'][0]), con)
# print(df.sort_values('idx'))

# with open('threadURLList.csv') as f:
#   reader = csv.reader(f)
#   links_thread = [row for row in reader]
# print(links_thread)

df = pd.read_csv('threadURLList.csv', encoding='shift-jis')
print(df)

# con.close()
