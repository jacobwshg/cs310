#
# PhotoApp API functions that interact with PhotoApp web service
# to support downloading and uploading images to S3, along with
# retrieving and updating data in associated photoapp database.
#
# Initial code (initialize, get_ping, get_users):
#	 Prof. Joe Hummel
#	 Northwestern University
#

import logging as lg
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from configparser import ConfigParser

import base64

#
# module-level variables:
#
WEB_SERVICE_URL = 'set via call to initialize()'

RETRY_DEF = retry(
	stop=stop_after_attempt(3), 
	wait=wait_exponential(multiplier=1, min=2, max=30),
	retry=retry_if_exception_type((ConnectionError, Timeout)),
	reraise=True
)

#
# Modularized logic for handling response errors
#
def catch_resp_error( response ):
	if response.status_code == 500:
		#
		# failed:
		#
		json = response.json()
		msg = json[ 'message' ]
		err_msg = f"status code {response.status_code}: {msg}"
		#
		# NOTE: this exception will not trigger retry mechanism, 
		# since we reached the server and the server-side failed, 
		# and we are assuming the server-side is also doing retries.
		#
		raise HTTPError(err_msg)

	elif response.status_code == 400:
		raise ValueError( response.json()[ 'message' ] )

	else:
		# 
		# something unexpected happened, and in this case we don't 
		# have a JSON-based response, so let Python raise proper
		# HTTPError for us:
		#
		response.raise_for_status()


###################################################################
#
# initialize
#
# Initializes local environment need to access PhotoApp web 
# service, based on given client-side configuration file. Call
# this function only once, and call before calling any other 
# API functions.
#
# NOTE: does not check to make sure we can actually reach the
# web service. Call get_ping() to check.
#
def initialize(client_config_file):
	"""
	Initializes local environment for AWS access, returning True
	if successful and raising an exception if not. Call this 
	function only once, and call before calling any other API
	functions.
	
	Parameters
	----------
	client_config_file is the name of the client-side configuration 
	file, probably 'photoapp-client-config.ini', which contains URL 
	for web service.
	
	Returns
	-------
	True if successful, raises an exception if not
	"""

	try:
		#
		# extract and save URL of web service for other API functions:
		#
		global WEB_SERVICE_URL

		configur = ConfigParser()
		configur.read(client_config_file)
		WEB_SERVICE_URL = configur.get('client', 'webservice')

		#
		# success:
		#
		return True

	except Exception as err:
		lg.error("initialize():")
		lg.error(str(err))
		raise


###################################################################
#
# get_ping
#
# To "ping" a system is to see if it's up and running. This 
# function pings the bucket and the database server to make
# sure they are up and running. Returns a tuple (M, N), where
#
#	 M = # of items in the photoapp bucket
#	 N = # of users in the photoapp.users table
#
# If an error occurs / a service is not accessible, an exception
# is raised. Exceptions of type HTTPError are from the underlying
# web service.
#
@RETRY_DEF
def get_ping():
	"""
	Based on the configuration file, retrieves the # of items in the S3 bucket and
	the # of users in the photoapp.users table. Both values are returned as a tuple
	(M, N). If an error occurs, e.g. S3 or the database is unreachable, then an 
	exception is raised.
	
	Parameters
	----------
	N/A
	
	Returns
	-------
	the tuple (M, N) where M is the # of items in the S3 bucket and
	N is the # of users in the photoapp.users table. If an error 
	occurs, e.g. S3 or the database is unreachable, then an exception
	is raised.
	"""

	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/ping"

		response = requests.get(url)

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			M = json['M']
			N = json['N']
			return (M, N)
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_ping():")
		lg.error(str(err))
		#
		# raise exception to trigger retry mechanism if appropriate:
		#
		raise

	finally:
		# nothing to do
		pass


###################################################################
#
# get_users
#
@RETRY_DEF
def get_users():
	"""
	Returns a list of all the users in the database. Each element 
	of the list is a tuple containing userid, username, givenname
	and familyname (in this order). The tuples are ordered by 
	userid, ascending. If an error occurs, an exception is raised.
	Exceptions of type HTTPError are from the underlying web service.
	
	Parameters
	----------
	N/A
	
	Returns
	-------
	a list of all the users, where each element of the list is a tuple
	containing userid, username, givenname, and familyname in that 
	order. The list is ordered by userid, ascending. On error an 
	exception is raised; exceptions of type HTTPError are from the 
	underlying web service.
	"""
	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/users"

		response = requests.get( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			rows = json['data']

			# 
			# rows is a dictionary-like list of objects, so
			# let's extract the values and discard the keys
			# to honor the API's return value:
			#
			users = []

			for row in rows:
				userid = row["userid"]
				username = row["username"]
				givenname = row["givenname"]
				familyname = row["familyname"]
				#
				user = (userid, username, givenname, familyname)
				users.append(user)

			return users
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_users():")
		lg.error(str(err))
		#
		# raise exception to trigger retry mechanism if appropriate:
		#
		raise

	finally:
		# nothing to do
		pass


###################################################################
#
# get_images
#
@RETRY_DEF
def get_images(userid = None):
	"""
	Returns a list of all the images in the database. Each element 
	of the list is a tuple containing assetid, userid, localname
	and bucketkey (in this order). The list is ordered by assetid, 
	ascending. If a userid is given, then just the images with that 
	userid are returned; validity of the userid is not checked, 
	which implies that an empty list is returned if the userid is 
	invalid. If an error occurs, an exception is raised. Exceptions 
	of type HTTPError are from the underlying web service.
	
	Parameters
	----------
	userid (optional) filters the returned images for just this userid
	
	Returns
	-------
	a list of images, where each element of the list is a tuple
	containing assetid, userid, localname, and bucketkey in that order.
	The list is ordered by assetid, ascending. If an error occurs, 
	an exception is raised. Exceptions of type HTTPError are from the 
	underlying web service.
	"""

	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/images"
		if userid:
			url += f"?userid={ userid }"

		response = requests.get( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			rows = json['data']

			#
			# rows is a dictionary-like list of objects, so
			# let's extract the values and discard the keys
			# to honor the API's return value:
			#
			imgs = []

			for row in rows:
				assetid	  = row["assetid"]
				userid    = row["userid"]
				localname = row["localname"]
				bucketkey = row["bucketkey"]
				#
				img = ( assetid, userid, localname, bucketkey )
				imgs.append( img )

			return imgs
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_images():")
		lg.error(str(err))
		raise

	finally:
		# nothing to do
		pass

###################################################################
#
# post_image
#
@RETRY_DEF
def post_image(userid, local_filename):
	"""
	Uploads an image to S3 with a unique name, allowing the same local
	file to be uploaded multiple times if desired. A record of this 
	image is inserted into the database, and upon success a unique
	assetid is returned to identify this image. The image is also 
	analyzed by Rekognition to label objects within
	the image; the results of this analysis are also saved in the
	database (and can be retrieved later via get_image_labels). If 
	an error occurs, an exception is raised. An invalid userid is 
	considered a ValueError, "no such userid". Exceptions of type 
	HTTPError are from the underlying web service.

	Parameters
	----------
	userid for whom we are uploading this image
	local filename of image to upload
	
	Returns
	-------
	image's assetid upon success, raises an exception on error
	"""
	try:
		baseurl = WEB_SERVICE_URL
		# `:` is only needed for JS-side formal parameter
		url = baseurl + "/image" + f"/{ userid }"

		#
		# first, open the file and read in the bytes
		#
		img_bytes = None
		with open( local_filename, 'rb' ) as infile:
			img_bytes = infile.read()

		#
		# now encode as base64 string, build msg to embed in the request,
		# and post request to the web service in JSON format:
		#
		enc_bytes = base64.b64encode( img_bytes )
		img_str   = enc_bytes.decode()

 		# strip parent directories for filename to be recorded in DB
		pure_localname = local_filename[ local_filename.rfind('/')+1: ]

		data = \
		{ \
			"local_filename": pure_localname, \
			"img_str": img_str, \
		}

		response = requests.post( url, json=data )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			assetid = json[ "assetid" ]
			return assetid
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("post_image():")
		lg.error(str(err))
		raise

	finally:
		# nothing to do
		pass

###################################################################
#
# get_image
#
@RETRY_DEF
def get_image(assetid, local_filename = None):
	"""
	Downloads the image from S3 denoted by the provided asset. If a
	local_filename is provided, the newly-downloaded file is saved
	with this filename (overwriting any existing file with this name).
	If a local_filename is not provided, the newly-downloaded file
	is saved using the local filename that was saved in the database
	when the file was uploaded. If successful, the filename for the
	newly-downloaded file is returned; if an error occurs then an
	exception is raised. An invalid assetid is considered a
	ValueError, "no such assetid". Exceptions of type HTTPError 
	are from the underlying web service.
	
	Parameters
	----------
	assetid of image to download
	local filename (optional) for newly-downloaded image
	
	Returns
	-------
	local filename for the newly-downloaded file, or raises an 
	exception upon error
	"""

	try:
		baseurl = WEB_SERVICE_URL
		url = baseurl + "/image" + f"/{ assetid }"

		response = requests.get( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			img_str = json[ "data" ]
			img_bytes = base64.b64decode( img_str )

			localname = \
				local_filename[ local_filename.rfind('/')+1: ] \
				if local_filename \
				else json[ "local_filename" ]

			with open( localname, "wb" ) as outfile:
				outfile.write( img_bytes )

			return localname

		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_image():")
		lg.error(str(err))
		raise

	finally:
		# nothing to do
		pass

###################################################################
#
# get_image_labels
#
def get_image_labels(assetid):
	"""
	When an image is uploaded to S3, Rekognition is
	automatically called to label objects in the image. Given the 
	image assetid, this function retrieves those labels. In
	particular this function returns a list of tuples. Each tuple
	is of the form (label, confidence), where label is a string 
	(e.g. 'sailboat') and confidence is an integer (e.g. 90).
	The tuples are ordered by label, ascending. If an error occurs
	an exception is raised; an invalid assetid is considered a
	ValueError, "no such assetid". Exceptions of type HTTPError 
	are from the underlying web service.

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

	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/image_labels" + f"/{ assetid }"

		response = requests.get( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			records = json['data']

			#
			# rows is a dictionary-like list of objects, so
			# let's extract the values and discard the keys
			# to honor the API's return value:
			#
			return [ ( rcd[ "label" ], rcd[ "confidence" ] ) for rcd in records ]

		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_image_labels():")
		lg.error(str(err))
		raise

	finally:
		# nothing to do
		pass


###################################################################
#
# get_images_with_label
#
def get_images_with_label(label):
	"""
	When an image is uploaded to S3, Rekognition is
	automatically called to label objects in the image. These labels
	are then stored in the database for retrieval / search. Given a
	label (partial such as 'boat' or complete 'sailboat'), this 
	function performs a case-insensitive search for all images with
	this label. The function returns a list of images, where each 
	element of the list is a tuple of the form (assetid, label, 
	confidence). The list is returned in order by assetid, and for
	all elements with the same assetid, ordered by label. If an 
	error occurs, an exception is raised. Exceptions of type 
	HTTPError are from the underlying web service.

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

	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/images_with_label" + f"/{ label }"

		response = requests.get( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			rows = json[ "data" ]
			return [ \
				( row[ 'assetid' ], row[ 'label' ], row[ 'confidence' ] ) \
				for row in rows \
			]
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("GIWL():")
		lg.error(str(err))
		#
		# raise exception to trigger retry mechanism if appropriate:
		#
		raise

	finally:
		# nothing to do
		pass


###################################################################
#
# delete_images
#
def delete_images():
	"""
	Delete all images and associated labels from the database and 
	S3. Returns True if successful, raises an exception on error.
	Exceptions of type HTTPError are from the underlying web service.
	The images are not deleted from S3 unless the database is 
	successfully cleared; if an error occurs either (a) there are
	no changes or (b) the database is cleared but there may be
	one or more images remaining in S3 (which has no negative 
	effect since they have unique names).

	Parameters
	----------
	N/A

	Returns
	-------
	True if successful, raises an exception on error
	"""

	try:
		baseurl = WEB_SERVICE_URL

		url = baseurl + "/images"

		response = requests.delete( url )

		if response.status_code == 200:
			#
			# success
			#
			json = response.json()
			msg = json[ 'message' ]
			return "success" in msg.lower();
		else:
			catch_resp_error( response )

	except Exception as err:
		lg.error("get_users():")
		lg.error(str(err))
		#
		# raise exception to trigger retry mechanism if appropriate:
		#
		raise

	finally:
		# nothing to do
		pass



