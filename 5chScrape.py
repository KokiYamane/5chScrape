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
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html5lib')
    thread = soup.find(class_='thread')
    if thread == None:
        return None

    dts = thread.find_all('dt')
    dds = thread.find_all('dd')
    idx = range(len(dts))

    posts = []
    for i, dt, dd in zip(idx, dts, dds):
        posts.append({
            'number': i+1,
            'datetime': getDatetime(dt.text),
            'userId': getId(dt.text),
            'userName': dt.find('b').text,
            'text': dd.text.replace('\n', '')
        })
    title = soup.find('title').text
    return {
        'title': title,
        'datetime': getDatetime(thread.find('dt').text),
        'url': url,
        'length': len(posts),
        'board': boardName,
        'content': posts
    }


def insertThread(folderName, thread):
    thread_id = 'thread_id_' + str(uuid.uuid4()).replace('-', '_')
    threadInfo = {}
    threadInfo['id'] = thread_id
    threadInfo['datetime'] = thread['datetime']
    threadInfo['url'] = thread['url']
    threadInfo['length'] = thread['length']
    threadInfo['board'] = thread['board']
    threadInfo['title'] = thread['title']
    json_thread_info = json.dumps(threadInfo, ensure_ascii=False)
    with open(folderName + '/threads.jsonl', mode='a', encoding='utf-8') as f:
        f.write(json_thread_info + '\n')

    threadsFolderName = folderName + '/threads'
    if not os.path.isdir(threadsFolderName):
        os.mkdir(threadsFolderName)
    with open(folderName + '/threads/' + thread_id + '.jsonl',
                mode='w', encoding='utf-8') as f:
        for post in thread['content']:
            json_thread = json.dumps(post, ensure_ascii=False)
            f.write(json_thread + '\n')


def saveThread(arg):
    url, board = arg
    folderName = '5chThreads'
    try:
        thread = scanThread(url, board)
        if thread == None:
            e = 'Thread Not Found'
            print('[Error] {} {}'.format(url, e))
            return
        insertThread(folderName, thread)
        print('get thread data: {} {} {} {}'.format(
            thread['datetime'], url, thread['board'], thread['title']))
    except Exception as e:
        print('[Error] {} {}'.format(url, e))


def main():
    df_links_threads = pd.read_csv('threadURLList.csv', encoding='shift-jis')
    df_links_threads = df_links_threads.set_index('url')
    print(df_links_threads.index)

    folderName = '5chThreads'
    if not os.path.isdir(folderName):
        os.mkdir(folderName)
    if os.path.isfile('5chThreads/threads.jsonl'):
        df_exist_threads = pd.read_json(
            '5chThreads/threads.jsonl', orient='records', lines=True)
        print(df_exist_threads['url'])

        urlList = np.setdiff1d(df_links_threads.index, df_exist_threads['url'])
        print('URL Number: ' + str(len(urlList)))
    else:
        urlList = df_links_threads.index

    args = []
    for url in urlList:
        args.append((url, df_links_threads['board'][url]))
    pprint.pprint(args[:10])

    with concurrent.futures.ProcessPoolExecutor(max_workers=61) as executor:
        thread = executor.map(saveThread, args)


if __name__ == '__main__':
    main()
