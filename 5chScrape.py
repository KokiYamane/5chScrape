import time
import random
import requests
from bs4 import BeautifulSoup
import json
import re
import datetime
import sqlite3
import uuid
import os
import csv
import concurrent.futures
import pandas as pd
import pprint

from LineNotify import *


def getDomain(url):
  return re.match(r'(?:https?://)?(?P<host>.*?)(?:[:#?/@]|$)', url).group(0)[:-1]

def getLinksFromBoard(url):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')
  board = soup.find(class_='board')
  if board == None: return []

  domain = getDomain(url)
  return [domain + link.get('href') for link in board.find_all('a')]

def getLinksFromMain(url):
  domain = getDomain(url)

  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')

  boardName = soup.find(class_='name_strings')
  board = ''
  if boardName != None: board = boardName.text

  threads = soup.find(class_='main')
  if threads == None: return []

  links = []
  for p in threads.find_all('p')[1:]:
    lines = p.find(class_='lines')
    if lines == None: continue
    if int(lines.text) < 100: continue
    link = p.find('a')
    if link == None: continue
    links.append(domain + link.get('href'))
  return board, links

def getDatetime(text):
  match_date = re.search(r'\d\d\/\d\d/\d\d', text)
  if match_date != None: text_date = match_date.group(0)
  else: return ''
  match_time = re.search(r'\d\d:\d\d', text)
  if match_time != None: text_time = match_time.group(0)
  else: return ''
  try:
    d = datetime.datetime.strptime(text_date + ' ' + text_time, '%y/%m/%d %H:%M')
    return d.strftime('%Y-%m-%d %H:%M:%S')
  except:
    return ''

def scanThread(url, boardName):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')
  thread = soup.find(class_='thread')
  if thread == None: return None

  dts = thread.find_all('dt')
  dds = thread.find_all('dd')
  idx = range(len(dts))

  posts = []
  for i, dt, dd in zip(idx, dts, dds):
    posts.append({
      'index': i,
      'user': dt.find('b').text,
      'datetime': getDatetime(dt.text),
      'post': dd.text
    })
  title = soup.find('title').text
  return {
    'title': title,
    'datetime': getDatetime(thread.find('dt').text),
    'board': boardName,
    'url': url,
    'length': len(posts),
    'content': posts
  }

def insertThread(con, thread):
  thread_id = 'thread_id_' + str(uuid.uuid4()).replace('-', '_')

  create_table = '''create table if not exists {} (idx int, datetime varchar(64), user varchar(64),
                    post varchar(64))'''.format(thread_id)
  con.execute(create_table)

  insert_sql = 'insert into {} (idx, datetime, user, post) values (?,?,?,?)'.format(thread_id)
  row = [(post['index'], post['datetime'], post['user'], post['post']) for post in thread['content']]
  con.executemany(insert_sql, row)

  create_table = '''create table if not exists threads (id varchar(64), datetime varchar(64), board varchar(64),
                    title varchar(64), url varchar(64), length int)'''
  con.execute(create_table)

  sql = 'insert into threads (id, datetime, board, title, url, length) values (?,?,?,?,?,?)'
  user = (thread_id, thread['datetime'], thread['board'], thread['title'], thread['url'], thread['length'])
  con.execute(sql, user)

  con.commit()


def saveThread(arg):
  connection, url, board = arg

  # print(cursor.execute(
  #     'SELECT url FROM threads WHERE NAME=?', url))
  # if cursor.execute('SELECT COUNT(*) FROM sqlite_master WHERE TYPE="table" AND NAME=?', url):
  #   return

  try:
    thread = scanThread(url, board)
    print('get thread data: {} {} {} {}'.format(
        thread['datetime'], url, thread['board'], thread['title']))
    # insertThread(connection, thread)
  except Exception as e:
    print('[Error] {} {}'.format(url, e))
    message = 'error\n\n{}\n\n{}'.format(e, url)
    # if lineNotify != None:
    #   lineNotify.send(message=message)

def main():
  # lineNotifyFlag = False
  # filename = 'LineAccessToken.txt'
  # if os.path.exists(filename):
  #   with open(filename) as f:
  #     LineAccessToken = f.read()
  #     lineNotify = LINENotify(access_token=LineAccessToken)
  #     lineNotifyFlag = True
  # else: print('Line Access Token Not Find.')

  dbFilename = '5chScrape.db'
  connection = sqlite3.connect(dbFilename)
  cursor = connection.cursor()

  df_links_thread = pd.read_csv('threadURLList.csv', encoding='shift-jis')
  print(df_links_thread)

  df_links_thread = df_links_thread.sample(frac=1).reset_index(drop=True)
  print(df_links_thread)

  args = []
  for url, board in zip(df_links_thread['url'], df_links_thread['board']):
    args.append((connection, url, board))
  pprint.pprint(args[:5])
  with concurrent.futures.ThreadPoolExecutor(max_workers=61) as executor:
    thread = executor.map(saveThread, args)


  connection.close()

if __name__ == '__main__': main()
