import requests
from bs4 import BeautifulSoup
import re
import pprint
import csv


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

def main():
  urlName = 'http://lavender.5ch.net/kakolog_servers.html'
  url = requests.get(urlName)
  soup = BeautifulSoup(url.content, 'html5lib')
  links = [link.get('href') for link in soup.find_all('a')]
  pprint.pprint(links)

  # links_board = sum([getLinksFromBoard(link) for link in links], [])
  # pprint.pprint(links_board)
  links_board = []
  for link in links:
    try:
      link_board = getLinksFromBoard(link)
      pprint.pprint(link_board)
      links_board.extend(link_board)
    except Exception as e:
      print(e)
  # links_board = sum(links_board, [])

  csv_threadURLList = open('threadURLList.csv', 'w', newline='')

  for link_board in links_board:
    try:
      board, links_thread = getLinksFromMain(link_board)
      rows = [links_thread, [board] * len(links_thread)]
      rows = [list(x) for x in zip(*rows)]
      pprint.pprint(rows)
      writer = csv.writer(csv_threadURLList)
      writer.writerows(rows)
    except Exception as e:
      print(e)


if __name__ == '__main__': main()
