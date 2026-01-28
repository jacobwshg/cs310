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
RETRY_3 = retry(
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
  @RETRY_3
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

###################################################################
#
# get_users
#
@RETRY_3
def get_users():
  """
  Returns a list of all the users in the database. Each element
  of the list is a tuple containing userid, username, givenname
  and familyname (in that order). The tuples are ordered by userid,
  ascending. If an error occurs, an exception is raised.

  Parameters
  ----------
  N/A

  Returns
  -------
  a list of all the users, where each element of the list is a tuple
  containing userid, username, givenname, and familyname in that order.
  The list is ordered by userid, ascending. On error an exception is
  raised.
  """
  query = """
    Select userid, username, givenname, familyname
    From users
    Order By userid Asc;
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
    raise

  return res

###################################################################
#
# get_images
#
@RETRY_3
def get_images(userid = None):
  """
  Returns a list of all the images in the database. Each element
  of the list is a tuple containing assetid, userid, localname
  and bucketkey (in this order). The list is ordered by assetid,
  ascending. If a userid is given, then just the images with that
  userid are returned; validity of the userid is not checked,
  which implies that an empty list is returned if the userid is
  invalid. If an error occurs, an exception is raised.

  Parameters
  ----------
  userid (optional) filters the returned images for just this userid

  Returns
  -------
  a list of images, where each element of the list is a tuple
  containing assetid, userid, localname, and bucketkey in that order.
  The list is ordered by assetid, ascending. If an error occurs,
  an exception is raised.
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

  res = None

  try:
    with get_dbConn() as dbconn:
      with dbconn.cursor() as cursor:
        if userid:
          cursor.execute( query, [ userid ] )
        else:
          cursor.execute( query )
        res = list( cursor.fetchall() )
  except Exception as err:
    lg.error( "get_images():" )
    lg.error( str(err) )
    raise

  return res

###################################################################
#
# post_image
#
def post_image(userid, local_filename):
  """
  Uploads an image to S3 with a unique name, allowing the same local
  file to be uploaded multiple times if desired. A record of this
  image is inserted into the database, and upon success a unique
  assetid is returned to identify this image. The image is also
  analyzed by the Rekognition AI slop to label objects within
  the image; the results of this analysis are also saved in the
  database (and can be retrieved later via get_image_labels). If
  an error occurs, an exception is raised. An invalid userid is
  considered a ValueError, "no such userid".

  Parameters
  ----------
  userid for whom we are uploading this image
  local filename of image to upload

  Returns
  -------
  image's assetid upon success, raises an exception on error
  """

  import uuid

  @RETRY_3
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
          cursor.execute( query, [ userid ] )
          match cursor.rowcount:
            case 1:
              username = cursor.fetchone()[0]
            case 0:
              raise ValueError( "no such userid" )
            case _:
              raise ValueError( "unexpected duplicate userid" )
    except Exception as err:
      lg.error( "post_image.lookup_user():" )
      lg.error( str( err ) )
      raise

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
    except Exception as err:
      lg.error( "post_image.upload_to_bucket():" )
      lg.error( str( err ) )
      raise
    finally:
      try:
        bkt.close()
      except:
        pass

    return bucketkey

  @RETRY_3
  def update_db( bucketkey ):
    success = False
    query = """
      Insert Into assets( userid, localname, bucketkey )
      Values( %s, %s, %s );
    """
    try:
      with get_dbConn() as dbconn:
        try:
          dbconn.begin()
          with dbconn.cursor() as cursor:
            cursor.execute( query, [ userid, local_filename, bucketkey ] )
          dbconn.commit()
          success = True
        except Exception as err:
          dbconn.rollback()
          lg.error( "post_image.update_db():" )
          lg.error( str( err ) )
          raise
    except Exception as err:
      lg.error( "post_image.update_db():" )
      lg.error( str( err ) )
      raise
    return success

  @RETRY_3
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
      raise

    return assetid

  def generate_labels( bucketkey ):
    labels = None
    try:
      bkt = get_bucket()
      rkg = get_rekognition()
      response = rkg.detect_labels(
        Image=\
        {
          'S3Object':\
          {
            'Bucket': bkt.name,
            'Name': bucketkey,
          },
        },
        MaxLabels=100,
        MinConfidence=80,
      )
      labels = response['Labels']

    except Exception as err:
      lg.error( "post_image.get_labels():" )
      lg.error( str( err ) )
      raise

    finally:
      try:
        bkt.close()
      except:
        pass
      try:
        rkg.close()
      except:
        pass

    return labels

  @RETRY_3
  def update_labels( assetid, bucketkey ):
    success = False

    query = """
      Insert Into labels( assetid, label, confidence )
      Values( %s, %s, %s );
    """

    try:
      labels = generate_labels( bucketkey )
      with get_dbConn() as dbconn:
        try:
          dbconn.begin()
          with dbconn.cursor() as cursor:
            for row in labels:
              cursor.execute(
                query, 
                [ assetid, row.get('Name'), int( row.get('Confidence') ) ]
              )
          dbconn.commit()
          success = True

        except Exception as err:
          dbconn.rollback()
          lg.error( "delete_images.update_labels():" )
          lg.error( str( err ) )
          raise

    except Exception as err:
      lg.error( "delete_images.update_labels():" )
      lg.error( str( err ) )
      raise

    return success

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

  if not update_labels( assetid, bucketkey ):
    return None

  return assetid

def get_image( assetid, local_filename=None ):
  """
  Downloads the image from S3 denoted by the provided asset. 

  If a local_filename is provided, the newly-downloaded file is saved with 
  this filename (overwriting any existing file with this name).

  If a local_filename is not provided, the newly-downloaded file is saved 
  using the local filename that was saved in the database
  when the file was uploaded. 

  If successful, the filename for the newly-downloaded file is returned; 
  if an error occurs then an exception is raised. An invalid assetid is considered 
  a ValueError, "no such assetid".

  Parameters
  ----------
  assetid of image to download
  local filename (optional) for newly-downloaded image

  Returns
  -------
  local filename for the newly-downloaded file, or raises an
  exception upon error
  """

  @RETRY_3
  def get_db_record():
    query = """
      Select localname, bucketkey
      From assets
      Where assetid = %s;
    """

    db_localname, bucketkey = None, None

    try:
      with get_dbConn() as dbconn:
        with dbconn.cursor() as cursor:
          cursor.execute( query, [ assetid ] )
      match cursor.rowcount:
        case 1:
          db_localname, bucketkey = cursor.fetchone()
        case 0:
          raise ValueError( "no such assetid" )
        case _:
          raise ValueError( "unexpected duplicate assetid" )
    except Exception as err:
      lg.error( "get_image.get_db_record():" )
      lg.error( str( err ) )
      raise

    return db_localname, bucketkey

  def get_file( bucketkey, localname ):
    success = False
    try:
      bkt = get_bucket()
      bkt.download_file( bucketkey, localname )
      success = True
    except Exception as err:
      lg.error( "get_image.get_file():" )
      lg.error( str( err ) )
      raise
    finally:
      try:
        bucket.close()
      except:
        pass

    return success

  db_localname, bucketkey = get_db_record()
  if not db_localname:
    return None

  localname = local_filename if local_filename else db_localname

  if not get_file( bucketkey, localname ):
    return None

  return localname


###################################################################
#
# delete_images
#
def delete_images():
  """
  Delete all images and associated labels from the database and S3.

  Returns True if successful, raises an exception on error.

  The images are not deleted from S3 unless the database is successfully 
  cleared; if an error occurs either 
  (a) there are no changes or 
  (b) the database is cleared but there may be one or more images 
  remaining in S3 (which has no negative effect since they have unique 
  names).

  Parameters
  ----------
  N/A

  Returns
  -------
  True if successful, raises an exception on error
  """

  @RETRY_3
  def getbucketkeys():
    query = """
      Select bucketkey
      From assets;
    """
    keys = None
 
    try:
      with get_dbConn() as dbconn:
        with dbconn.cursor() as cursor:
          cursor.execute( query )
          rows = cursor.fetchall()
          keys = [ { 'Key': row[0] } for row in rows ]
    except Exception as err:
      lg.error( "delete_images.getbucketkeys():" )
      lg.error( str( err ) )
      raise

    #if len( keys ) == 0:
    #  return None

    return keys

  @RETRY_3
  def clear_db():
    query = """
      SET foreign_key_checks = 0;

      TRUNCATE TABLE assets;
      TRUNCATE TABLE labels;

      SET foreign_key_checks = 1;
      ALTER TABLE assets AUTO_INCREMENT = 1001;
    """
    success = False

    try:
      with get_dbConn() as dbconn:
        try:
          dbconn.begin()
          with dbconn.cursor() as cursor:
            cursor.execute( query )
          dbconn.commit()
          success = True
        except Exception as err:
          dbconn.rollback()
          lg.error( "delete_images.clear_db():" )
          lg.error( str( err ) )
          raise

    except Exception as err:
      lg.error( "delete_images.clear_db():" )
      lg.error( str( err ) )
      raise
    return success

  def clear_bucket( bucketkeys ):
    success = False
    try:
      bkt = get_bucket()
      bkt.delete_objects( Delete={ 'Objects': bucketkeys } )
      success = True
    except Exception as err:
      lg.error( "delete_images.clear_bucket():" )
      lg.error( str( err ) )
      raise
    finally:
      try:
        bucket.close()
      except:
        pass

    return success

  bucketkeys = getbucketkeys()
  if not bucketkeys:
    return False

  if not clear_db():
    return False

  if not clear_bucket( bucketkeys ):
    return False

  return True

###################################################################
#
# get_image_labels
#
@RETRY_3
def get_image_labels( assetid ):
  """
  When an image is uploaded to S3, the Rekognition AI slop is
  automatically called to label objects in the image. Given the
  image assetid, this function retrieves those labels. In
  particular this function returns a list of tuples. Each tuple
  is of the form (label, confidence), where label is a string
  (e.g. 'sailboat') and confidence is an integer (e.g. 90).
  The tuples are ordered by label, ascending. If an error occurs
  an exception is raised; an invalid assetid is considered a
  ValueError, "no such assetid".

  Parameters
  ----------
  image assetid to retrieve labels for

  Returns
  -------
  a list of labels identified in the image, where each element
  of the list is a tuple of the form (label, confidence) where
  label is a string and confidence is an integer. If an error
  occurs an exception is raised; an invalid assetid is considered
  a ValueError, "no such assetid".
  """

  labels = []

  aid_query = """
    Select assetid 
    From assets
    Where assetid = %s;
  """

  labels_query = """
    Select label, confidence
    From labels
    Where assetid = %s
    Order By label Asc;
  """

  try:
    with get_dbConn() as dbconn:
      with dbconn.cursor() as cursor:

        cursor.execute( aid_query, [ assetid ] )
        if cursor.rowcount < 1:
          raise ValueError( "no such assetid" )

        cursor.execute( labels_query, [ assetid ] )
        labels = list( cursor.fetchall() )

  except Exception as err:
    lg.error( "get_image_labels():" )
    lg.error( str( err ) )
    raise
 
  return labels

###################################################################
#
# get_images_with_label
#
@RETRY_3
def get_images_with_label( label ):
  """
  When an image is uploaded to S3, the Rekognition AI slop is
  automatically called to label objects in the image. These labels
  are then stored in the database for retrieval / search. Given a
  label (partial such as 'boat' or complete 'sailboat'), this
  function performs a case-insensitive search for all images with
  this label. The function returns a list of images, where each
  element of the list is a tuple of the form 
  (assetid, label, confidence). 
  The list is returned in order by assetid, and for
  all elements with the same assetid, ordered by label. If an
  error occurs, an exception is raised.

  Parameters
  ----------
  label to search for, this can be a partial word (e.g. 'boat')

  Returns
  -------
  a list of images that contain this label, even partial matches.
  Each element of the list is a tuple (assetid, label, confidence)
  where assetid identifies the image, label is a string, and
  confidence is an integer. The list is returned in order by
  assetid, and for all elements with the same assetid, ordered
  by label. If an error occurs, an exception is raised.
  """

  res = None

  pattern = f"%{label}%"

  query = """
    Select assetid, label, confidence
    From labels
    Where label Like %s
    Order By assetid Asc, label Asc; 
  """

  try:
    with get_dbConn() as dbconn:
      with dbconn.cursor() as cursor:
        cursor.execute( query, [ pattern ] )
        res = list( cursor.fetchall() )
  except Exception as err:
    lg.error( "get_images_with_label():" )
    lg.error( str( err ) )
    raise

  return res

