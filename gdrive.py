from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import json
import os
import urllib.parse


def writeGDrive(drive, title, parentId, string):
  title_encorded = urllib.parse.quote(title)
  fileList = drive.ListFile({
    'q': "title = '{}' and '{}' in parents and trashed=false".format(title_encorded, parentId)
  }).GetList()

  if len(fileList) == 0:
    f = drive.CreateFile({'title': title, 'parents': [{'id':parentId}]})
  else: f = fileList[0]

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

def getFolderIdList(filename, drive):
  if os.path.exists(filename):
    folderIdListFile = open(filename, 'r')
    folderIdList = json.load(folderIdListFile)
    return folderIdList

  folderId_5chScrape = getFolderIDOnGDrive(drive, '5chScrape')

  folderIdList = {}
  for year in range(1999, 2021):
    folderIdList[str(year)] = getFolderIDOnGDrive(drive, str(year), parentId=folderId_5chScrape)
    for month in range(1, 13):
      folderName = str(year) + str(month).zfill(2)
      folderIdList[folderName] = getFolderIDOnGDrive(drive, folderName, parentId=folderIdList[str(year)])
      print('get folderId: ' + folderName)
  print(folderIdList)
  json_folderIdList = json.dumps(folderIdList, ensure_ascii=False, indent=2)
  with open(filename, mode='w') as f:
    f.write(json_folderIdList)

  return folderIdList