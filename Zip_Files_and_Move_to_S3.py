__author__ = 'dja410'

import os, math
import psycopg2
import datetime
from boto.s3.connection import S3Connection
import boto
from filechunkio import FileChunkIO
import threading

working_directory = '\\\\Nyrofcswnfp06\\eng_sys_apps\\For_Conversion_Use_Only' + \
                    '\\Verizon_Guava\\FME_CONVERSION\\ReadyForMainModuleProcessing\\'
s3_working_directory = 'FME_Processing/ToBeProcessed/'


def getzippedwirecenters(directory):
    if directory.endswith('RubberSheeterControlDataOutput') or directory.endswith('Input_data'):
        wclist = {}
        for file in os.listdir(directory):
            if file.endswith('.zip'):
                wc = file[0:7]
                try:
                    wclist[wc] = 'Zipped'
                except Exception, e:
                    logfile.write(str(datetime.datetime.now()) + '\t' +
                                  'An error occurred with wirecenter ' + wc + ': ' + str(e))
                    continue
        return wclist


def gets3bucketlist(connection, directory):
    bucket = connection.get_bucket('frontier-singapore', validate=False)
    keys = bucket.list(prefix=directory)
    return keys


def getallwcsizes(connection):
    dicWcSizes = {}
    cur = connection.cursor()
    cur.execute("select " +
                "substring(jurwc,1,2) || '_' || substring(jurwc,3,4) as wc, " +
                "case " +
                "    when total_ipids >=30000 then 'large' " +
                "    else 'small' " +
                "end as wc_size " +
                "from wire_centers")
    rows = cur.fetchall()
    for row in rows:
        wc = row[0]
        size = row[1]
        try:
            dicWcSizes[wc] = size
        except:
            continue
    return dicWcSizes


def createpostgresconnection():
    try:
        conn = psycopg2.connect("dbname='tracker_2015' user='sandbox1' host='10.112.91.15' password='pwd'")
    except:
        print "I am unable to connect to the database"
    return conn


def uploadfiletos3(srcfile, destkey):
    source_path = srcfile
    source_size = os.stat(source_path).st_size
    s3conn = boto.s3.connect_to_region('ap-southeast-1',
                                       aws_access_key_id='key',
                                       aws_secret_access_key='secret',
                                       calling_format = boto.s3.connection.OrdinaryCallingFormat()
                                       )
    b = s3conn.get_bucket('bucket', validate=False)

    # Create a multipart upload request
    mp = b.initiate_multipart_upload(destkey)

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

logfile = open(working_directory + 'LogFiles' + '\\transferLog_' +
               str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '.log', 'w+')


gdrivefolders = ['RubberSheeterControlDataOutput', 'Input_data']
s3wcsizes = ['large', 'small']
dicExistingRubberSheetZipFiles = {}
dicExistingInputDataZipFiles = {}
dicExistingS3RubberSheetZipFiles = {}
dicExistingS3InputData = {}

######### Need to add logic to zip completed WC data here ##########

for folder in gdrivefolders:
    try:
        path = working_directory + folder
        logfile.write(str(datetime.datetime.now()) + '\t' +
                      'Creating Dictionary of ' + folder + ' Zipped Wirecenters in Directory: ' + path + '\n')
        if folder == 'RubberSheeterControlDataOutput':
            dicExistingRubberSheetZipFiles = getzippedwirecenters(path)
        elif folder == 'Input_data':
            dicExistingInputDataZipFiles = getzippedwirecenters(path)
    except Exception, e:
        logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')

for size in s3wcsizes:
    for folder in gdrivefolders:
        try:
            path = s3_working_directory + size + '/' + folder + '/'
            logfile.write(str(datetime.datetime.now()) + '\t' +
                          'Creating Dictionary of ' + size + ' ' + folder +
                          ' S3 Wirecenters in Directory: ' + path + '\n')
            if folder == 'RubberSheeterControlDataOutput':
                rss3rubbersheet = gets3bucketlist(s3conn, path)
                for item in rss3rubbersheet:
                    try:
                        if not item.name == '':
                            wc = item.name.split('/')[4][0:7]
                            dicExistingS3RubberSheetZipFiles[wc] = size
                    except:
                        continue
            elif folder == 'Input_data':
                rss3inputdata = gets3bucketlist(s3conn, path)
                for item in rss3inputdata:
                    try:
                        if not item.name == '':
                            wc = item.name.split('/')[4][0:7]
                            dicExistingS3InputData[wc] = size
                    except:
                        continue
        except Exception, e:
            logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')


conn = createpostgresconnection()
dicWirecentersAndSizes = getallwcsizes(conn)
dicUploadInputDataS3 = {}
dicUploadRubberSheetDataS3 = {}

for key in dicWirecentersAndSizes:
    if key not in dicExistingS3InputData.keys():
        if key in dicExistingInputDataZipFiles.keys():
            dicUploadInputDataS3[key] = dicWirecentersAndSizes[key]

    if key not in dicExistingS3RubberSheetZipFiles.keys():
        if key in dicExistingRubberSheetZipFiles.keys():
            dicUploadRubberSheetDataS3[key] = dicWirecentersAndSizes[key]

logfile.write(str(datetime.datetime.now()) + '\t' + "Beginning Upload to S3\n")
i = 0
rsthreads = []
idthreads = []


for key in dicUploadRubberSheetDataS3:
    wcsize = dicUploadRubberSheetDataS3[key]
    logfile.write(str(datetime.datetime.now()) + '\t' + "Uploading " + str(key) +
                      ", RubberSheet, " + str(wcsize) + " to S3\n")
    srcfile = working_directory + '\\' + 'RubberSheeterControlDataOutput\\' + \
              str(key) + '_RubberSheeterControlDataOutput.zip'
    destkey = s3_working_directory + '/' + wcsize + '/RubberSheeterControlDataOutput/' + \
              str(key) + '_RubberSheeterControlDataOutput.zip'
    t = threading.Thread(target=uploadfiletos3, args=(srcfile, destkey)) #.start()
    rsthreads.append(t)

for x in rsthreads:
        x.start()

for y in rsthreads:
        y.join()


for key in dicUploadInputDataS3:
        wcsize = dicUploadInputDataS3[key]
        logfile.write(str(datetime.datetime.now()) + '\t' + "Uploading " + str(key) +
                      ", InputData, " + str(wcsize) + " to S3\n")
        srcfile = working_directory + '\\' + 'Input_data\\' + \
                  str(key) + '_Input_data.zip'
        destkey = s3_working_directory + '/' + wcsize + '/Input_data/' + \
                  str(key) + '_Input_data.zip'
        t = threading.Thread(target=uploadfiletos3, args=(srcfile, destkey)) #.start()
        idthreads.append(t)

for a in idthreads:
        a.start()

for b in idthreads:
        b.join()

logfile.write(str(datetime.datetime.now()) + '\t' + "Finished Uploading Files.")
logfile.close()
print "Finished"