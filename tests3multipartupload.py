__author__ = 'dja410'

import zipfile
import os, math
import psycopg2
import datetime
from boto.s3.connection import S3Connection
import boto
from filechunkio import FileChunkIO


def uploadfiletos3(connection, srcdirectory, destdirectory):
    source_path = srcdirectory
    source_size = os.stat(source_path).st_size
    b = connection.get_bucket('bucket', validate=False)

# Create a multipart upload request
    mp = b.initiate_multipart_upload(destdirectory)

# Use a chunk size of 50 MiB (feel free to change this)
    chunk_size = 52428800   #from example
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

# Send the file parts, using FileChunkIO to create a file-like object
# that points to a certain byte range within the original file. We
# set bytes to never exceed the original file size.
    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)
    mp.complete_upload()

s3conn = boto.s3.connect_to_region('ap-southeast-1',
                                   aws_access_key_id='key',
                                   aws_secret_access_key='secret',
                                   calling_format = boto.s3.connection.OrdinaryCallingFormat()
                                   )

gdrive = '\\\\Nyrofcswnfp06\\eng_sys_apps\\For_Conversion_Use_Only' +\
         '\\Verizon_Guava\\FME_CONVERSION\\ReadyForMainModuleProcessing\\RubberSheeterControlDataOutput\\' +\
         'C5_4416_RubberSheeterControlDataOutput.zip'

s3key = 'FME_Processing/ToBeProcessed/small/' +\
        'RubberSheeterControlDataOutput/C5_4416_RubberSheeterControlDataOutput.zip'

uploadfiletos3(s3conn, gdrive, s3key)

print "Finished!"
