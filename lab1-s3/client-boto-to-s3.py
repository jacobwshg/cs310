#
# Downloads image from S3 using AWS's boto3 library
#
import logging
import sys

import boto3  # access to Amazon Web Services (AWS)
from botocore import UNSIGNED
from botocore.client import Config

from configparser import ConfigParser

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
      config=Config(signature_version=UNSIGNED))

    bucket = s3.Bucket(bucket_name)

    #
    # Download image requested by user:
    #
    imagename = input("Enter image to download (e.g. 'degu.jpg')> ")

    local_filename = imagename  # same name locally

    bucket.download_file(imagename, local_filename)
    
    print("Success, image downloaded to '", local_filename, "'")

    print()
    print("**Done**")

except Exception as err:
  print()
  print("ERROR:", str(err))
