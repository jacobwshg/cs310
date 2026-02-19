#
# Simple client-side testing code for photoapp API functions.
#
# Initial code:
#	 Prof. Joe Hummel
#	 Northwestern University
#

import photoapp
import logging
import sys


###################################################################
#
# main
#

#
# eliminate traceback so we just get error message:
#
sys.tracebacklimit = 0

#
# capture logging output in file 'log.txt'
#
logging.basicConfig(
	filename='log.txt',
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	filemode='w'
)

def init():
	print("initializing:")
	success = photoapp.initialize('photoapp-client-config.ini')
	print(success)

	if not success:
		print("**ERROR: photoapp failed to initialize, check log for errors")
		sys.exit(0)

	print()

#
# get_ping:
#
def run_ping():
	try:
		print("**get_ping:")
		(M,N) = photoapp.get_ping()
		print(f"M: {M}")
		print(f"N: {N}")

	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# get_users:
#
def run_get_users():
	try:
		print("**get_users:")
		users = photoapp.get_users()

		for user in users:
			print(user)
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# post_image:
#
def run_post_img( userid=80001 ):
	try:
		path = "./chicago.jpg"
		print(f"**post_image: { userid } { path } ")
		assetid = photoapp.post_image( userid, path )
		print( assetid )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()
		

#
# get_images:
#
def run_get_imgs():
	try:
		print("**get_images:")
		imgs = photoapp.get_images()

		for img in imgs:
			print( img )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

def run_get_imgs_uid( userid=80001 ):
	try:
		userid = 80001
		print(f"**get_images with userid { userid }:")
		imgs = photoapp.get_images( userid )

		for img in imgs:
			print( img )

	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

def run_get_imgs_bad_uid():
	try:
		userid = "melchizedek solomonovich"
		print(f"**get_images with userid { userid }:")
		imgs = photoapp.get_images( userid )

		for img in imgs:
			print( img )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# get_image:
#
def run_get_img( assetid=1001 ):
	try:
		assetid = 1001
		print(f"**get_image: { assetid } ")
		local_filename = photoapp.get_image( assetid )

		print( local_filename )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

def run_get_img_localname( assetid = 1001, localname="out_img.jpg" ):
	try:
		print(f"**get_image: { assetid } { localname } ")
		local_filename = photoapp.get_image( assetid, local_filename=localname )

		print( local_filename )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

def run_get_img_bad_aid( assetid = 607 ):
	try:
		print(f"**get_image: { assetid } ")
		local_filename = photoapp.get_image( assetid )

		print( local_filename )
		
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# delete_images:
#
def run_del_imgs():
	try:
		print("**delete_images: ")
		res = None	
		res = photoapp.delete_images()
		print( res )

	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# get_image_labels:
#
def run_get_labels( assetid=1001 ):
	try:
		print(f"**get_image_labels: { assetid } ")
		res = None
		res = photoapp.get_image_labels( assetid )
		print( res )
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()

#
# g..._i_...w_...l...:
#

def run_GIWL( label='a' ):
	try:
		print(f"**GIWL: label { label } ")
		res = None
		res = photoapp.get_images_with_label( label )
		print( res )
	except Exception as err:
		print("CLIENT ERROR:")
		print(str(err))

	finally:
		print()


if __name__ == "__main__":

	#
	# run and test:
	#
	print()
	print("**starting**")
	print()

	init()
	run_ping()

	"""

	run_get_users()

	run_post_img()

	run_get_imgs()
	run_get_imgs_uid( )
	run_get_imgs_bad_uid()

	run_get_img()
	run_get_img_localname(  )
	run_get_img_bad_aid( assetid="1001; Select * From Assets" )

	run_get_labels( 1001 )
	
	run_GIWL( label='a' )

	"""

	run_del_imgs()
	
	print()
	print("**done**")
	print()

