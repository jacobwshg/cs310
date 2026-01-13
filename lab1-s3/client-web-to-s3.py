#
# Calls S3 to download images from bucket
#
# Prof. Joe Hummel
# Northwestern University
#

#!/bin/python3.12

import requests
from configparser import ConfigParser
import xml.etree.ElementTree as xmlET
import time

def file_suffix_from_resp( response ):
    conttype = response.headers.get( 'Content-Type', None )
    if not conttype:
        return None
    labels = conttype.split( sep='/' )
    suffix = '.unknown'
    if len( labels ) < 2:
        return suffix

    if labels[0] == 'image':
        if labels[1] == 'jpeg':
                suffix = '.jpg'
    elif labels[0] == 'text':
        if labels[1] == 'plain':
            suffix = '.txt'
        elif labels[1].startswith( 'x-python' ):
            suffix = '.py'
    elif labels[0] == 'application':
        if labels[1] in [ 'json', 'pdf', 'xml', 'zip' ]:
            suffix = '.' + labels[1]

    return suffix

def error_msg_from_resp( response ):
    e_msg = xmlET.fromstring( response.text ).find( 'Message' ).text
    return e_msg

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
#imagename = input("Enter image to download (e.g. 'degu.jpg')> ")
imagename = input("Enter image to download without extension> ").strip()

#
# Error/retry test
#
#endpoint += '.42'

url = endpoint + "/" + imagename

#print( url )

MAX_RETRIES = 3
for retry in range( 1, MAX_RETRIES+1 ):
    try:
        response = requests.get( url )
        stat = response.status_code
        if stat == 200 or stat == 404:
            break
    except Exception as exc:
        if ( retry == MAX_RETRIES ):
            response = requests.models.Response()
            response.status_code = -1
            root = xmlET.Element( 'Error' )
            xmlET.SubElement( root, 'Message' ).text = str( exc )
            response._content = xmlET.tostring( root, encoding='UTF-8' )
    finally:
        pass
    time.sleep(retry)

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

    # restore file suffix
    if imagename.find( '.' ) < 0:
        suffix = file_suffix_from_resp( response )
        if suffix:
            imagename += suffix

    with open(imagename, 'wb') as file:
        file.write(response.content)
    print("Success, image downloaded to '", imagename, "'")
else:
    #
    # error:
    #
    e_msg = error_msg_from_resp( response )
    print("ERROR:")
    print(" URL: " + url)
    print(" Msg: " + e_msg)

print()
print("**Done**")

