from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import requests
from bs4 import BeautifulSoup
import json
import re
import datetime


def writeGDrive(drive, title, parentId, string):
  f = drive.CreateFile({'title': title, 'parents': [{'id': parentId}]})
  f.SetContentString(string)
  f.Upload()

def getFolderIDOnGDrive(drive, folderName, parentId='root'):
  fileList = drive.ListFile({
    'q': 'title = "' + folderName + '" and' +
         'mimeType = "application/vnd.google-apps.folder" and' +
         'trashed=false and' +
         '"{}" in parents'.format(parentId),
    'maxResults': 100,
  }).GetList()

  if len(fileList) == 0:
    folder = drive.CreateFile({'title': folderName,
                               'mimeType': 'application/vnd.google-apps.folder',
                               'parents': [{'id': parentId}]})
    folder.Upload()
    return folder['id']

  return fileList[0]['id']

def getLinksFromBoard(url):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')
  board = soup.find(class_='board')
  if board == None: return []
  return ['https://1999.5ch.net' + link.get('href') for link in board.find_all('a')]

def getLinksFromMain(url):
  html = requests.get(url)
  soup = BeautifulSoup(html.content, 'html5lib')

  boardName = soup.find(class_='name_strings')
  tag = ''
  if boardName != None: tag = boardName.text

  threads = soup.find(class_='main')
  if threads == None: return []

  links = []
  for p in threads.find_all('p')[1:]:
    lines = p.find(class_='lines')
    if lines == None: continue
    if int(lines.text) < 100: continue
    link = p.find('a')
    if link == None: continue
    links.append('https://1999.5ch.net' + link.get('href'))
  return tag, links

def getDatetime(text):
  text_date = re.search(r'\d\d\d\d\/\d\d/\d\d', text).group(0)
  text_time = re.search(r'\d\d:\d\d', text).group(0)
  d = datetime.datetime.strptime(text_date + ' ' + text_time, '%Y/%m/%d %H:%M')
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

  folderId_5chScrape = getFolderIDOnGDrive(drive, '5chScrape')

  folderIdList = {}
  for year in range(1999, 2021):
    folderIdList[str(year)] = getFolderIDOnGDrive(drive, str(year), parentId=folderId_5chScrape)
    for month in range(1, 13):
      folderName = str(year) + str(month).zfill(2)
      folderIdList[folderName] = getFolderIDOnGDrive(drive, folderName, parentId=folderIdList[str(year)])
      print('get folderId: ' + folderName)
  print(folderIdList)

  urlName = 'http://lavender.5ch.net/kakolog_servers.html'
  url = requests.get(urlName)
  soup = BeautifulSoup(url.content, 'html5lib')
  links = [link.get('href') for link in soup.find_all('a')][:1]

  links_board = sum([getLinksFromBoard(link) for link in links], [])[5:]
  # i = 0
  for link_board in links_board:
    tag, links_thread = getLinksFromMain(link_board)

    for link_thread in links_thread:
      thread = getThread(link_thread, tag)
      if thread == None: continue
      print('get thread data from ' + link_thread)
      json_thread = json.dumps(thread, ensure_ascii=False, indent=2)
      folderId = folderIdList[getFolderName(thread)]
      writeGDrive(drive, makeFilename(thread), folderId, json_thread)
      # i += 1
      # if i > 1000: break

if __name__ == '__main__': main()