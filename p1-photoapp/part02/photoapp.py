#
# PhotoApp API functions, supporting downloading and uploading images to S3,
# along with retrieving and updating data in associated photoapp database.
#
# Initial code (initialize, get_ping, get_* helper functions):
#   Prof. Joe Hummel
#   Northwestern University
#

import logging
import pymysql
import os
import boto3

from botocore.client import Config
from configparser import ConfigParser

from tenacity import retry, stop_after_attempt, wait_exponential

#
# module-level varibles:
#
PHOTOAPP_CONFIG_FILE = 'set via call to initialize()'

# retry config
RETRY3 = retry(
  stop=stop_after_attempt( 3 ),
  wait=wait_exponential(multiplier=1, min=2, max=30),
  reraise=True
)

###################################################################
#
# get_dbConn
#
# create and return connection object, based on configuration
# information in app config file. You should call close() on 
# the object when you are done.
#
def get_dbConn():
  """
  Reads the configuration info from app config file, creates
  pymysql connection object based on this info, and returns it.
  You should call close() on the object when you are done.

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
    configur = ConfigParser()
    configur.read(PHOTOAPP_CONFIG_FILE)

    endpoint = configur.get('rds', 'endpoint')
    portnum = int(configur.get('rds', 'port_number'))
    username = configur.get('rds', 'user_name')
    pwd = configur.get('rds', 'user_pwd')
    dbname = configur.get('rds', 'db_name')

    #
    # now create connection object and return it:
    #
    dbConn = pymysql.connect(
      host=endpoint, port=portnum,
      user=username,
      passwd=pwd,
      database=dbname,
      #
      # allow execution of a query string with multiple SQL queries:
      #
      client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS
    )

    return dbConn
  
  except Exception as err:
    logging.error("get_dbconn():")
    logging.error(str(err))
    raise


###################################################################
#
# get_bucket
#
# create and return bucket object, based on configuration
# information in app config file. You should call close() 
# on the object when you are done.
#
def get_bucket():
  """
  Reads the configuration info from app config file, creates
  a bucket object based on this info, and returns it. You 
  should call close() on the object when you are done.

  Parameters
  ----------
  N/A

  Returns
  -------
  S3 bucket object
  """

  try:
    #
    # configure S3 access using config file:
    #  
    configur = ConfigParser()
    configur.read(PHOTOAPP_CONFIG_FILE)
    bucketname = configur.get('s3', 'bucket_name')
    regionname = configur.get('s3', 'region_name')

    s3 = boto3.resource(
           's3',
           region_name=regionname,
           config = Config(
             retries = \
             {
               'max_attempts': 3,
               'mode': 'standard'
             }
           )
         )

    bucket = s3.Bucket(bucketname)

    return bucket
  
  except Exception as err:
    logging.error("get_bucket():")
    logging.error(str(err))
    raise
  

###################################################################
#
# get_rekognition
#
# create and return rekognition object, based on configuration
# information in app config file. You should call close() on
# the object when you are done.
#
def get_rekognition():
  """
  Reads the configuration info from app config file, creates
  a rekognition object based on this info, and returns it.
  You should call close() on the object when you are done.

  Parameters
  ----------
  N/A

  Returns
  -------
  Rekognition object
  """

  try:
    #
    # configure S3 access using config file:
    #  
    configur = ConfigParser()
    configur.read(PHOTOAPP_CONFIG_FILE)
    regionname = configur.get('s3', 'region_name')

    rekognition = boto3.client(
                    'rekognition', 
                    region_name=regionname,
                    config = Config(
                      retries = {
                        'max_attempts': 3,
                        'mode': 'standard'
                      }
                    )
                  )

    return rekognition
  
  except Exception as err:
    logging.error("get_rekognition():")
    logging.error(str(err))
    raise


###################################################################
#
# initialize
#
# Initializes local environment need to access AWS, based on
# given configuration file and user profiles. Call this function
# only once, and call before calling any other API functions.
#
# NOTE: does not check to make sure we can actually reach and
# login to S3 and database server. Call get_ping() to check.
#
def initialize(config_file, s3_profile, mysql_user):
  """
  Initializes local environment for AWS access, returning True
  if successful and raising an exception if not. Call this 
  function only once, and call before calling any other API
  functions.
  
  Parameters
  ----------
  config_file is the name of configuration file, probably 'photoapp-config.ini'
  s3_profile to use for accessing S3, probably 's3readwrite'
  mysql_user to use for accessing database, probably 'photoapp-read-write'
  
  Returns
  -------
  True if successful, raises an exception if not
  """

  try:
    #
    # save name of config file for other API functions:
    #
    global PHOTOAPP_CONFIG_FILE
    PHOTOAPP_CONFIG_FILE = config_file

    #
    # configure boto for S3 access, make sure we can read necessary
    # configuration info:
    #
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

    boto3.setup_default_session(profile_name=s3_profile)

    configur = ConfigParser()
    configur.read(config_file)
    bucketname = configur.get('s3', 'bucket_name')
    regionname = configur.get('s3', 'region_name')

    #
    # also check to make sure we can read database server config info:
    #
    endpoint = configur.get('rds', 'endpoint')
    portnum = int(configur.get('rds', 'port_number'))
    username = configur.get('rds', 'user_name')
    pwd = configur.get('rds', 'user_pwd')
    dbname = configur.get('rds', 'db_name')

    if username == mysql_user:
      # we have password, all is good:
      pass
    else:
      raise ValueError("mysql_user does not match user_name in [rds] section of config file")
    
    #
    # success:
    #
    return True

  except Exception as err:
    logging.error("initialize():")
    logging.error(str(err))
    raise


###################################################################
#
# get_ping
#
# To "ping" a system is to see if it's up and running. This 
# function pings the bucket and the database server to make
# sure they are up and running. Returns a tuple (M, N), where
#
#   M = # of items in the photoapp bucket
#   N = # of users in the photoapp.users table
#
# If an error occurs / a service is not accessible, M or N
# will be an error message. Hopefully the error messages will
# convey what is going on (e.g. no internet connection).
#
def get_ping():
  """
  Based on the configuration file, retrieves the # of items in the S3 bucket and
  the # of users in the photoapp.users table. Both values are returned as a tuple
  (M, N), where M or N are replaced by error messages if an error occurs or a
  service is not accessible.
  
  Parameters
  ----------
  N/A
  
  Returns
  -------
  the tuple (M, N) where M is the # of items in the S3 bucket and
  N is the # of users in the photoapp.users table. If S3 is not
  accessible then M is an error message; if database server is not
  accessible then N is an error message.
  """

  def get_M():
    try:
      #
      # access S3 and obtain the # of items in the bucket:
      #
      bucket = get_bucket()

      assets = bucket.objects.all()

      M = len(list(assets))
      return M

    except Exception as err:
      logging.error("get_ping.get_M():")
      logging.error(str(err))
      raise

    finally:
      try:
        bucket.close()
      except:
        pass

  '''
  @retry(
    stop=stop_after_attempt( 3 ),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True
  )
  '''
  @RETRY3
  def get_N():
    try:
      #
      # create connection to MySQL database server and then
      # execute query to retrieve # of users:
      #
      dbConn = get_dbConn()
      dbCursor = dbConn.cursor()

      sql = """
            SELECT count(userid) FROM users;
            """
      
      dbCursor.execute(sql)
      row = dbCursor.fetchone()

      #
      # we get back a tuple with one result in it:
      #
      N = row[0]
      return N

    except Exception as err:
      logging.error("get_ping.get_N():")
      logging.error(str(err))
      raise
    
    finally:
      try: 
        dbCursor.close()
      except: 
        pass
      try:
        dbConn.close()
      except:
        pass

  #
  # we compute M and N separately so that we can do separate exception
  # handling, and thus get partial results if one succeeds and one fails:
  #
  try:
    M = get_M()
  except Exception as err:
    M = str(err)

  try:
    N = get_N()
  except Exception as err:
    N = str(err)

  return (M, N)

import logging as lg

@RETRY3
def get_users():
  """
  Return a list of all users in the database.
  Each element in the list is a tuple containing
  (userid, username, givennamem, familyname).
  The tuples are ordered by userid, ascending. 
  """
  query = """
    Select userid, username, givenname, familyname
    From users
    Order by userid Asc;
  """

  res = []

  try:
    with get_dbConn() as dbconn:
      with dbconn.cursor() as cursor:
        cursor.execute( query )
        res = list( cursor.fetchall() )
  except Exception as err:
    lg.error( "get_users():" )
    lg.error( str(err) )

  return res

@RETRY3
def get_images( userid=None ):
  """
  Returns a list of all the images in the database.  
  Each element of the list is a tuple containing 
  (assetid, userid, localname, bucketkey).
  The list is ordered by assetid, ascending.
  If a userid is given, then just the images with that userid are returned.
  """

  query = """
    Select assetid, userid, localname, bucketkey
    From assets
  """

  if userid is not None:
    query += """
      Where userid = %s
    """

  query += """
    Order By assetid Asc;
  """

  res = []

  try:
    with get_dbConn() as dbconn:
      with dbconn.cursor() as cursor:
        cursor.execute( query )
        res = list( cursor.fetchall() )
  except Exception as err:
    lg.error( "get_images():" )
    lg.error( str(err) )

  return res

def post_image( userid, local_filename ):
  """
  Uploads an image to S3 with a unique name, allowing the same local file
  to be uploaded multiple times if desired.
  A record of this image is inserted into the database, and upon success
  a unique assetid is returned to identify this image.

  An invalid userid is considered a ValueError, "no such userid".
  """

  import uuid

  @RETRY3
  def lookup_user():
    username = None
    query = """
       Select username
       From users
       Where userid = %s;
    """
    try:
      with get_dbConn() as dbconn:
        with dbconn.cursor() as cursor:
          cursor.execute( query, [userid] )
          match cursor.rowcount:
            case 1:
              username = cursor.fetchone()[0]
            case 0:
              raise ValueError('no such userid')
            case _:
              raise ValueError('duplicate userid')
    except Exception as err:
      lg.error( "post_image.lookup_user():" )
      lg.error( str( err ) )

    return username

  def upload_to_bucket( username ):
    bucketkey = None
    try:
      bkt = get_bucket() 
      _bktkey = ''.join(
        [
          username, 
          '/',
          str( uuid.uuid4() ),
          '-',
          local_filename
        ]
      )
      bkt.upload_file( local_filename, _bktkey )
      bucketkey = _bktkey
      #i = 2//0 
    except Exception as err:
      lg.error( "post_image.upload_to_bucket():" )
      lg.error( str( err ) )
    finally:
      try:
        bkt.close()
      except:
        pass

    return bucketkey

  @RETRY3
  def update_db( bucketkey ):
    success = False
    query = """
      Insert Into assets( userid, localname, bucketkey )
      Values( %s, %s, %s );
    """
    try:
      with get_dbConn() as dbconn:
        with dbconn.cursor() as cursor:
          dbconn.begin()
          try:
            cursor.execute( query, [ userid, local_filename, bucketkey ] )
            dbconn.commit()
            success = True
          except Exception as err:
            dbconn.rollback()
            lg.error( "post_image.update_db():" )
            lg.error( str( err ) )
    except Exception as err:
      lg.error( "post_image.update_db():" )
      lg.error( str( err ) )
    return success

  @RETRY3
  def retrieve_assetid( bucketkey ):
    assetid = None
    query = """
      Select assetid
      From assets
      Where bucketkey = %s;   
    """
    try:
      with get_dbConn() as dbconn:
        with dbconn.cursor() as cursor:
          cursor.execute( query, [ bucketkey ] )
          if cursor.rowcount == 1:
            assetid = cursor.fetchone()[0]
    except Exception as err:
      lg.error( "post_image.retrieve_assetid():" )
      lg.error( str( err ) )

    return assetid

  local_filename = local_filename.strip( './\\' ) 

  username = lookup_user()
  if not username:
    return None

  bucketkey = upload_to_bucket( username )
  if not bucketkey:
    return None

  if not update_db( bucketkey ):
    return None

  assetid = retrieve_assetid( bucketkey )
  if not assetid:
    return None

  return assetid

