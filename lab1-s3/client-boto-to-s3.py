#
# Downloads image from S3 using AWS's boto3 library
#

import boto3  # access to Amazon Web Services (AWS)
from botocore import UNSIGNED
from botocore.client import Config

from configparser import ConfigParser

import logging
import sys

def mk_suffix( conttype ):
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

#
# eliminate traceback so we just get error message:
#
sys.tracebacklimit = 0

try:
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
    bucket_name = configur.get('bucket', 'bucket_name')
    region_name = configur.get('bucket', 'region_name')

    #
    # gain access to CS 310's public photoapp bucket:
    #
    s3 = boto3.resource(
        's3',
        region_name=region_name,
        # enables access to public objects:
        config=Config(
            retries={ 'max_attempts': 3, 'mode': 'standard' },
            signature_version=UNSIGNED
        )
    )

    bucket = s3.Bucket(bucket_name)

    #
    # Download image requested by user:
    #
    imagename = input("Enter image to download without extension> ")

    metadata = s3.Object( bucket_name, imagename )

    local_filename = imagename  # same name locally
    if imagename.find( '.' ) < 0:
        local_filename += mk_suffix( metadata.content_type )

    bucket.download_file(imagename, local_filename)

    print() 
    print("Success, image downloaded to '" + local_filename + "'")

    print()
    print("**Done**")

except Exception as err:
    print()
    print("ERROR:")
    print( " Bucket: " + bucket_name ),
    print( " Region: " + region_name ),
    print( " Msg: ", str(err))

