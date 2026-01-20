#
# shorten.py
#
# Implements API for a URL shortening service
#
# Original author:
#   Prof. Joe Hummel
#   Northwestern University
#

import pymysql
from configparser import ConfigParser


###################################################################
#
# get_dbConn
#
# create and return connection object, based on configuration
# information in shorten-config.ini
#
def get_dbConn():
  """
  Reads the configuration info from shorten-config.ini, creates
  a pymysql connection object based on this info, and returns it

  Parameters
  ----------
  N/A

  Returns
  -------
  pymysql connection object
  """

  try:
    #
    # obtain database server config info:
    #
    config_file = 'shorten-config.ini'    
    configur = ConfigParser()
    configur.read(config_file)

    endpoint = configur.get('rds', 'endpoint')
    portnum = int(configur.get('rds', 'port_number'))
    username = configur.get('rds', 'user_name')
    pwd = configur.get('rds', 'user_pwd')
    dbname = configur.get('rds', 'db_name')

    #
    # now create connection object and return it:
    #
    dbConn = pymysql.connect(host=endpoint,
                            port=portnum,
                            user=username,
                            passwd=pwd,
                            database=dbname)

    return dbConn
  
  except Exception as err:
    print("**ERROR in shorten.get_dbConn():")
    print(str(err))
    return None
  

###################################################################
#
# get_url
#
# Looks up the short url in the database, returning the associated
# long url. Each time this is done, the count for that url is 
# incremented.
#
def get_url(shorturl):
  """
  Looks up the short url in the database, returning the associated
  long url. Each time this is done, the count for that url is 
  incremented.

  Parameters
  ----------
  shorturl : the short URL to lookup (string)

  Returns
  -------
  long URL (string), or empty string if short URL not found
  """

  select_query = """
    Select longurl 
    From shorten 
    Where shorturl = %s;
  """
  update_query = """
    Update shorten 
    Set access_cnt = access_cnt + 1 
    Where shorturl = %s;
  """
 
  with get_dbConn() as dbconn:
    with dbconn.cursor() as cursor:

      longurl = ""

      try:
        dbconn.begin()
        cursor.execute( select_query, [shorturl] )
        if cursor.rowcount == 1:
          row = cursor.fetchone()
          longurl = row[0]
          cursor.execute( update_query, [shorturl] )
        dbconn.commit()
        return longurl

      except Exception as err:
        dbconn.rollback()
        print("**ERROR in shorten.get_url():")
        print(str(err))
        return ""

  return ""

##################################################################
#
# get_stats
#
# Returns the count for the given short url, which represents
# the # of times the short url has been looked up
#
def get_stats(shorturl):
  """
  Looks up the short url and returns the count

  Parameters
  __________
  shorturl : the short URL to lookup (string)

  Returns
  _______
  the count associated with the short url, -1 if short URL not found
  """

  select_query = """
    Select access_cnt
    From shorten
    Where shorturl = %s;
  """

  with get_dbConn() as dbconn:
    with dbconn.cursor() as cursor:

      access_cnt = -1

      try:
        dbconn.begin()
        cursor.execute( select_query, [shorturl] )
        if cursor.rowcount == 1:
          row = cursor.fetchone()
          access_cnt = row[0]
        return access_cnt

      except Exception as err:
        print("**ERROR in shorten.get_stats():")
        print(str(err))
        return -1

  return -1

##################################################################
#
# put_shorturl:
#
# Maps the long url to the short url by inserting both urls
# into the database, returning True if successful. If the
# short url already exists in the database (and is mapped to
# a different long url), the database is left unchanged and
# False is returned since the short url is already taken.
#
def put_shorturl(longurl, shorturl):
  """
  Maps the long url to the short url by inserting both urls
  into the database with a count of 0. Fails if the short
  url is already taken AND mapped to a different long url.

  Parameters
  __________
  the original long URL (string)
  the desired short URL (string)

  Returns
  _______
  True if successful, False if not
  """

  # test if SHORTURL is in table
  select_query = """
    Select longurl
    From shorten
    Where shorturl = %s;
  """

  insert_query = """
    Insert Into shorten(shorturl, longurl, access_cnt)
    Values(%s, %s, 0);
  """

  with get_dbConn() as dbconn:
    with dbconn.cursor() as cursor:

      try:
        dbconn.begin()
        cursor.execute( select_query, [shorturl] )
        if cursor.rowcount > 0:
          for row in cursor.fetchall():
            if row[0] == longurl:
              return True
          return False
        else:
          cursor.execute( insert_query, [shorturl, longurl] )
        dbconn.commit()
        return True

      except Exception as err:
        dbconn.rollback()
        print("**ERROR in shorten.put_shorturl():")
        print(str(err))
        return False

  return False

###############################################################
#
# put_reset
#
# Deletes all the urls from the database
#
def put_reset():
  """
  Deletes all the urls from the database

  Parameters
  __________
  N/A

  Returns
  _______
  True if successful, False if not
  """

  delete_query = """
    Delete 
    From shorten;
  """

  with get_dbConn() as dbconn:
    with dbconn.cursor() as cursor:

      try:
        dbconn.begin()
        cursor.execute( delete_query )
        dbconn.commit()
        return True
      except Exception as err:
        print("**ERROR in shorten.put_reset():")
        print(str(err))
        return False

  return False

