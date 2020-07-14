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
import numpy as np

from LineNotify import *


def getDomain(url):
    return re.match(r'(?:https?://)?(?P<host>.*?)(?:[:#?/@]|$)',
                    url).group(0)[:-1]


def getLinksFromBoard(url):
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html5lib')
    board = soup.find(class_='board')
    if board == None:
        return []

    domain = getDomain(url)
    return [domain + link.get('href') for link in board.find_all('a')]


def getLinksFromMain(url):
    domain = getDomain(url)

    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html5lib')

    boardName = soup.find(class_='name_strings')
    board = ''
    if boardName != None:
        board = boardName.text

    threads = soup.find(class_='main')
    if threads == None:
        return []

    links = []
    for p in threads.find_all('p')[1:]:
        lines = p.find(class_='lines')
        if lines == None:
            continue
        if int(lines.text) < 100:
            continue
        link = p.find('a')
        if link == None:
            continue
        links.append(domain + link.get('href'))
    return board, links


def getId(text):
    match = re.search(r'ID:.{8}', text)
    if match != None:
        return match.group(0).replace('ID:', '')
    else:
        return ''


def getDatetime(text):
    match_date = re.search(r'\d\d\/\d\d/\d\d', text)
    if match_date != None:
        text_date = match_date.group(0)
    else:
        return ''
    match_time = re.search(r'\d\d:\d\d', text)
    if match_time != None:
        text_time = match_time.group(0)
    else:
        return ''
    try:
        d = datetime.datetime.strptime(
            text_date + ' ' + text_time, '%y/%m/%d %H:%M')
        return d.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ''


def scanThread(url, boardName):
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html5lib')
    thread = soup.find(class_='thread')
    if thread == None:
        return None

    dts = thread.find_all('dt')
    dds = thread.find_all('dd')
    idx = range(len(dts))

    posts = []
    for i, dt, dd in zip(idx, dts, dds):
        posts.append({
            'index': i+1,
            'user': dt.find('b').text,
            'id': getId(dt.text),
            'datetime': getDatetime(dt.text),
            'text': dd.text
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


def insertThread(dbFilename, thread):
    con = sqlite3.connect(dbFilename)

    create_table = '''create table if not exists threads (id varchar(64),
          datetime varchar(64), board varchar(64),
          title varchar(64), url varchar(64), length int)'''
    con.execute(create_table)

    thread_id = 'thread_id_' + str(uuid.uuid4()).replace('-', '_')
    insert_sql = '''insert into threads (id, datetime, board, title, url,
                  length)values (?,?,?,?,?,?)'''
    row = (thread_id, thread['datetime'], thread['board'],
           thread['title'], thread['url'], thread['length'])
    con.execute(insert_sql, row)

    create_table = '''create table if not exists {} (idx int,
                    datetime varchar(20), user varchar(64),
                    id varchar(8), text varchar(64))'''.format(thread_id)
    con.execute(create_table)

    insert_sql = '''insert into {} (idx, datetime, user, id, text)
                  values (?,?,?,?,?)'''.format(
        thread_id)
    rows = [(post['index'], post['datetime'], post['user'], post['id'],
              post['text']) for post in thread['content']]
    con.executemany(insert_sql, rows)

    con.commit()

    con.close()


def saveThread(arg):
    url, board = arg
    dbFilename = '5chThreads.sqlite3'
    try:
        thread = scanThread(url, board)
        print('get thread data: {} {} {} {}'.format(
            thread['datetime'], url, thread['board'], thread['title']))
        insertThread(dbFilename, thread)
    except Exception as e:
        print('[Error] {} {}'.format(url, e))


def main():
    df_links_threads = pd.read_csv('threadURLList.csv', encoding='shift-jis')
    df_links_threads = df_links_threads.set_index('url', )
    print(df_links_threads.index)

    dbFilename = '5chThreads.sqlite3'
    con = sqlite3.connect(dbFilename)
    create_table = '''create table if not exists threads (id varchar(64),
          datetime varchar(64), board varchar(64),
          title varchar(64), url varchar(64), length int)'''
    con.execute(create_table)
    df_exist_threads = pd.read_sql_query('SELECT * FROM threads ORDER BY title', con)
    print(df_exist_threads['url'])

    urlList = np.setdiff1d(df_links_threads.index, df_exist_threads['url'])
    print('URL Number: ' + str(len(urlList)))

    args = []
    for url in urlList:
        args.append((url, df_links_threads['board'][url]))
    pprint.pprint(args[:10])

    with concurrent.futures.ThreadPoolExecutor(max_workers=61) as executor:
        thread = executor.map(saveThread, args)


if __name__ == '__main__':
    main()
