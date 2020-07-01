import sqlite3
import pprint

con = sqlite3.connect('5chScrape.db')

cursor = con.cursor()

# cursor.execute('SELECT * FROM {} ORDER BY user'.format('しりとりしましょう！'))
cursor.execute('SELECT * FROM threads ORDER BY title')
threads = cursor.fetchall()
pprint.pprint(threads)

cursor.execute('SELECT * FROM {} ORDER BY user'.format(threads[10][0]))
thread = cursor.fetchall()
pprint.pprint(thread)

con.close()