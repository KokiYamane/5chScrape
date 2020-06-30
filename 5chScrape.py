from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import requests
from bs4 import BeautifulSoup
import json
import re
import datetime

from gdrive import *
from LineNotify import *


def getDomain(url):
  return re.match(r'(?:https?://)?(?P<host>.*?)(?:[:#?/@]|$)', url).group(0)

def getLinksFromBoard(url):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')
  board = soup.find(class_='board')
  if board == None: return []

  domain = getDomain(url)
  return [domain[:-1] + link.get('href') for link in board.find_all('a')]

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
  d = datetime.datetime.strptime(text_date + ' ' + text_time, '%y/%m/%d %H:%M')
  return d.strftime('%Y-%m-%d %H:%M:%S')

def getThread(url, boardName):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')
  thread = soup.find(class_='thread')
  if thread == None: return None

  posts = []
  for dt, dd in zip(thread.find_all('dt'), thread.find_all('dd')):
    posts.append({
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
    'content': posts
    }

def makeFilename(thread):
  d = datetime.datetime.strptime(thread['datetime'], '%Y-%m-%d %H:%M:%S')
  return d.strftime('%Y%m%d_%H%M') + '_' + thread['title'] + '.json'

def getFolderName(thread):
  d = datetime.datetime.strptime(thread['datetime'], '%Y-%m-%d %H:%M:%S')
  return d.strftime('%Y%m')

def main():
  gauth = GoogleAuth()
  gauth.LocalWebserverAuth()

  drive = GoogleDrive(gauth)

  lineNotifyFlag = False
  filename = 'LineAccessToken.txt'
  if os.path.exists(filename):
    with open(filename) as f:
      LineAccessToken = f.read()
      lineNotify = LINENotify(access_token=LineAccessToken)
      lineNotifyFlag = True
  else: print('Line Access Token Not Find.')

  folderIdList = getFolderIdList('folderIdList.json', drive)

  urlName = 'http://lavender.5ch.net/kakolog_servers.html'
  url = requests.get(urlName)
  soup = BeautifulSoup(url.content, 'html5lib')
  links = [link.get('href') for link in soup.find_all('a')]

  for link in links:
    links_board = getLinksFromBoard(link)
    for link_board in links_board:
      board, links_thread = getLinksFromMain(link_board)
      for link_thread in links_thread:
        thread = getThread(link_thread, board)
        if thread == None: continue
        print('get thread data: {} {} {} {}'.format(thread['board'], thread['datetime'], link_thread, thread['title']))
        json_thread = json.dumps(thread, ensure_ascii=False, indent=2)
        folderId = folderIdList[getFolderName(thread)]
        writeGDrive(drive, makeFilename(thread), folderId, json_thread)
        message = 'get thread data\n{}'.format(link_thread)
        if lineNotifyFlag: lineNotify.send(message=message)

if __name__ == '__main__': main()