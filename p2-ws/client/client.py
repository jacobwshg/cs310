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

#
# run and test:
#
print()
print("**starting**")
print()

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
try:
	print("**get_ping:")
	(M,N) = photoapp.get_ping()
	print(f"M: {M}")
	print(f"N: {N}")

except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

#
# get_users:
#
try:
	print("**get_users:")
	users = photoapp.get_users()

	for user in users:
		print(user)
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

#
# post_image:
#
try:
	userid = 80001
	path = "./chicago.jpg"
	print(f"**post_image: { userid } { path } ")
	assetid = photoapp.post_image( userid, path )
	print( assetid )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

#
# get_images:
#
try:
	print("**get_images:")
	imgs = photoapp.get_images()

	for img in imgs:
		print( img )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

try:
	userid = 80001
	print(f"**get_images with userid { userid }:")
	imgs = photoapp.get_images( userid )

	for img in imgs:
		print( img )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()


try:
	userid = "melchizedek solomonovich"
	print(f"**get_images with userid { userid }:")
	imgs = photoapp.get_images( userid )

	for img in imgs:
		print( img )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

#
# get_image:
#
try:
	assetid = 1001
	print(f"**get_image: { assetid } ")
	local_filename = photoapp.get_image( assetid )

	print( local_filename )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

try:
	assetid = 1002
	localname = "out_img.jpg"
	print(f"**get_image: { assetid } { localname } ")
	local_filename = photoapp.get_image( assetid, local_filename=localname )

	print( local_filename )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

try:
	assetid = 607
	print(f"**get_image: { assetid } ")
	local_filename = photoapp.get_image( assetid )

	print( local_filename )
		
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()

#
# delete_images:
#
try:
	print("**delete_images: ")
	res = None	
	res = photoapp.delete_images()
	print( res )
except Exception as err:
	print("CLIENT ERROR:")
	print(str(err))

print()



print()

#
# done:
#
print()
print("**done**")
print()

