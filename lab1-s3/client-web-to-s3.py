#
# Calls S3 to download images from bucket
#
# Prof. Joe Hummel
# Northwestern University
#

import requests
from configparser import ConfigParser

print("**Starting**")
print()

#
# setup AWS based on config file:
#
config_file = 's3-config.ini'    
configur = ConfigParser()
configur.read(config_file)

#
# get web server URL from config file:
#
endpoint = configur.get('webserver', 'endpoint')

#
# Call S3 web server to download image requested by user:
#
imagename = input("Enter image to download (e.g. 'degu.jpg')> ")

url = endpoint + "/" + imagename

response = requests.get(url)

#
# process the response:
#
status_code = response.status_code

print()
print('status code:', status_code)
print()

if status_code == 200:
  #
  # success, write image to a local file so we can view:
  #
  file = open(imagename, 'wb')
  file.write(response.content)
  file.close()
  print("Success, image downloaded to '", imagename, "'")
  
else:
  #
  # error:
  #
  print("ERROR:", response.text)

print()
print("**Done**")
