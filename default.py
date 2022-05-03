import datetime
import json as jsoninterface
import sqlite3
import xml.etree.ElementTree as ET
import mysql.connector
import xbmc
import xbmcvfs
import xbmcaddon
import xbmcgui

MIN_VIDEODB_VERSION = 116
MAX_VIDEODB_VERSION = 119  # i.e. Matrix, aka Kodi 19

advanced_file = xbmcvfs.translatePath('special://profile/advancedsettings.xml')
db_path = xbmcvfs.translatePath('special://database')
__addon_name__ = 'script.database.cleanup'

cache_paths = {}

def log(msg):
    xbmc.log((u"### [%s] - %s" % (str(__addon_name__), str(msg))), level=xbmc.LOGDEBUG)

def advanced_file_exits():
  if xbmcvfs.exists(advanced_file):
    log('Found advancedsettings.xml')
    return True
  else:
    return False  

def get_mysql_settings():
  try:
    advanced_settings = ET.parse(advanced_file)
  except ET.ParseError:
    log('Error parsing advancedsettings.xml file', xbmc.LOGERROR)
    return None
  xml_root = advanced_settings.getroot()
  for videodb in xml_root.findall('videodatabase'):
      try:
          mysql_host = videodb.find('host').text
      except:
          log('Unable to find MySQL host address')
          return None
      try:
          mysql_username = videodb.find('user').text
      except:
          log('Unable to determine MySQL username')
          return None
      try:
          mysql_password = videodb.find('pass').text
      except:
          log('Unable to determine MySQL password')
          return None
      try:
          mysql_port = videodb.find('port').text
      except:
          log('Unable to find MySQL port')
          return None
      try:
          mysql_dbname = videodb.find('name').text
      except:
          mysql_dbname = 'MyVideos'
      log('MySQL details - %s, %s, %s, %s' % (mysql_host, mysql_port, mysql_username, mysql_dbname))
      return { 'host': mysql_host, 'user': mysql_username, 'password': mysql_password, 'port': mysql_port, 'name': mysql_dbname }
  return None

def find_and_connect_mysql_database(database_settings):
    for num in range(MAX_VIDEODB_VERSION, MIN_VIDEODB_VERSION, -1):
      testname = database_settings['name'] + str(num)
      try:
        log('Attempting MySQL connection to %s' % testname)
        db_conn = mysql.connector.connect(user=database_settings['user'], database=testname, password=database_settings['password'], host=database_settings['host'], port=database_settings['port'])
        if db_conn.is_connected():
          mysql_dbname = testname
          log('Connected to MySQL database %s' % mysql_dbname)
          break
      except:
          pass

    if not db_conn.is_connected():
      raise Exception("Error - couldn't connect to MySQL database - %s " % s)

    return [mysql_dbname, db_conn]

def find_and_connect_sqlite_database():
    sqlite_dbname = 'MyVideos'

    for num in range(MAX_VIDEODB_VERSION, MIN_VIDEODB_VERSION, -1):
        testname = sqlite_dbname + str(num)
        our_test = db_path + testname + '.db'

        log('Checking for local database %s' % testname)
        if xbmcvfs.exists(our_test):
            break
    if num != MIN_VIDEODB_VERSION:
        sqlite_dbname = testname

    if sqlite_dbname == 'MyVideos': raise Exception('No video database found')
    log('Database name is %s' % sqlite_dbname)

    db_conn = sqlite3.connect(db_path + sqlite_dbname + '.db')
    
    return [sqlite_dbname, db_conn]

def execute_sql(cursor, sql, params=None, write = False):
  if not params: params = []
  cursor.execute(sql, params)
  if write: database['connection'].commit()

def exists_dir(id, path = None):
  if id in cache_paths:
    return cache_paths[id]
  else:
    if path == None: raise Exception("Parent %s not found" % id)
    cache_paths[id] = xbmcvfs.exists(path)
    return cache_paths[id]

def delete_path(path_info):
  execute_sql(db_cursor, "SELECT idPath, strPath, idParentPath FROM path WHERE idParentPath = %s" % database['replstr'], [path_info[0]])
  all_children = db_cursor.fetchall()
  for child in all_children:
    delete_path(child)
  execute_sql(db_cursor, "SELECT idFile, strFileName FROM files WHERE idPath = %s" % database['replstr'], [path_info[0]])
  all_files = db_cursor.fetchall()
  log("Deleting path %s" % path_info[1])
  execute_sql(db_cursor, "DELETE FROM path WHERE idPath = %s" % database['replstr'], [path_info[0]], True)
  for _file in all_files:
    # continue
    log("Deleting file %s" % _file[1])
    execute_sql(db_cursor, "DELETE FROM files WHERE idFile = %s" % database['replstr'], [_file[0]], True)

if advanced_file_exits():
  mysql_settings = get_mysql_settings()
  if mysql_settings:
    mysql_db = find_and_connect_mysql_database(mysql_settings)
    database = { 'type': 'mysql', 'filename': mysql_db.pop(0), 'replstr': '%s', 'connection': mysql_db.pop(0) }
  else:
    sqlite_db = find_and_connect_sqlite_database()
    database = { 'type': 'sqlite', 'filename': sqlite_db.pop(0), 'replstr': '?', 'connection': sqlite_db.pop(0) }
else:
    sqlite_db = find_and_connect_sqlite_database()
    database = { 'type': 'sqlite', 'filename': sqlite_db.pop(0), 'replstr': '?', 'connection': sqlite_db.pop(0) }

db_cursor = database['connection'].cursor()

execute_sql(db_cursor, "SELECT idPath, strPath, idParentPath FROM path")
all_paths = db_cursor.fetchall()
log("Checking path existense..")
for path in all_paths: exists_dir(path[0], path[1])
log("Non exists path with existing parent:")
for path in all_paths:
  if not exists_dir(path[0]) and (path[2] == None or exists_dir(path[2])): 
    delete_path(path)

# SELECT idEpisode FROM `episode` LEFT JOIN files ON episode.idFile = files.idFile WHERE files.idFile IS NULL;
# SELECT idMovie FROM `movie` LEFT JOIN files ON movie.idFile = files.idFile WHERE files.idFile IS NULL;