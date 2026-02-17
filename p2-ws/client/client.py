#
# Simple client-side testing code for photoapp API functions.
#
# Initial code:
#   Prof. Joe Hummel
#   Northwestern University
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

#
# done:
#
print()
print("**done**")
print()

