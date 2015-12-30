__author__ = 'dja410'

import zipfile
import os
import datetime
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto
import threading

working_directory = '\\d$\\ICGS\\'
landing_directory = '\\z$\\S3_Zipfiles\\'
s3_working_directory = 'FME_Processing/ToBeProcessed/'


def getwirecenters(directory):
    if directory.endswith('RubberSheeterControlDataOutput') or directory.endswith('Input_data'):
        wclist = {}
        for file in os.listdir(directory):
            if not file.endswith('.zip'):
                wc = file[0:7]
                try:
                    wclist[wc] = 'Exists'
                except Exception, e:
                    logfile.write(str(datetime.datetime.now()) + '\t' +
                                  'An error occurred with wirecenter ' + wc + ' in directory ' +
                                  str(directory) + ': ' + str(e))
                    continue
        return wclist


def gets3bucketlist(directory):
    connection = boto.s3.connect_to_region('ap-southeast-1',
                                           aws_access_key_id='key',
                                           aws_secret_access_key='secret',
                                           calling_format=boto.s3.connection.OrdinaryCallingFormat()
                                           )
    bucket = connection.get_bucket('bucket', validate=False)
    keys = bucket.list(prefix=directory)
    connection.close()
    return keys


def downloadfilefroms3(srckey, destfile):
    source_path = srckey
    s3conn = boto.s3.connect_to_region('ap-southeast-1',
                                       aws_access_key_id='key',
                                       aws_secret_access_key='secret',
                                       calling_format = boto.s3.connection.OrdinaryCallingFormat()
                                       )
    b = s3conn.get_bucket('bucket', validate=False)
    key = Key(b, source_path)
    try:
        key.get_contents_to_filename(destfile)
    except Exception, e:
        logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')

logfile = open(working_directory + 'LogFiles' + '\\transferLog_' +
               str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '.log', 'w+')


ddrivefolders = ['RubberSheeterControlDataOutput', 'Input_data']
s3wcsizes = ['large', 'small']
ec2servers = ['WIN-V335BOR09FA', 'WIN-V335BOR09F8']
dicExistingRubberSheetFiles = {}
dicExistingInputDataFiles = {}
dicExistingS3RubberSheetFiles = {}
dicExistingS3InputData = {}


for server in ec2servers:
    for folder in ddrivefolders:
        try:
            path = '\\\\' + server + '\\' + working_directory + folder
            logfile.write(str(datetime.datetime.now()) + '\t' +
                          'Creating Dictionary of ' + folder + ' Zipped Wirecenters in Directory: ' + path + '\n')
            if folder == 'RubberSheeterControlDataOutput':
                dicExistingRubberSheetFiles = getwirecenters(path)
            elif folder == 'Input_data':
                dicExistingInputDataFiles = getwirecenters(path)
        except Exception, e:
            logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')

for size in s3wcsizes:
    for folder in ddrivefolders:
        try:
            path = s3_working_directory + size + '/' + folder + '/'
            logfile.write(str(datetime.datetime.now()) + '\t' +
                          'Creating Dictionary of ' + size + ' ' + folder +
                          ' S3 Wirecenters in Directory: ' + path + '\n')
            if folder == 'RubberSheeterControlDataOutput':
                rss3rubbersheet = gets3bucketlist(path)
                for item in rss3rubbersheet:
                    try:
                        if not item.name == '':
                            wc = item.name.split('/')[4][0:7]
                            dicExistingS3RubberSheetFiles[wc] = size
                    except:
                        continue
            elif folder == 'Input_data':
                rss3inputdata = gets3bucketlist(path)
                for item in rss3inputdata:
                    try:
                        if not item.name == '':
                            wc = item.name.split('/')[4][0:7]
                            dicExistingS3InputData[wc] = size
                    except:
                        continue
        except Exception, e:
            logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')


dicDownloadInputDataS3 = {}
dicDownloadRubberSheetDataS3 = {}

for key in dicExistingS3InputData:
    if key not in dicExistingInputDataFiles.keys():
        dicDownloadInputDataS3[key] = dicExistingS3InputData[key]

for key in dicExistingS3RubberSheetFiles:
    if key not in dicExistingRubberSheetFiles.keys():
        dicDownloadRubberSheetDataS3[key] = dicExistingS3RubberSheetFiles[key]

logfile.write(str(datetime.datetime.now()) + '\t' + "Beginning Download from S3\n")
i = 0
rsthreads = []
idthreads = []


for key in dicDownloadRubberSheetDataS3:
    wcsize = dicDownloadRubberSheetDataS3[key]
    logfile.write(str(datetime.datetime.now()) + '\t' + "Downloading " + str(key) +
                      ", RubberSheet, " + str(wcsize) + " from S3\n")
    srckey = s3_working_directory + '/' + wcsize + '/RubberSheeterControlDataOutput/' + \
              str(key) + '_RubberSheeterControlDataOutput.zip'
    destfile = landing_directory + '\\' + 'RubberSheeterControlDataOutput\\' + \
              str(key) + '_RubberSheeterControlDataOutput.zip'
    t = threading.Thread(target=downloadfilefroms3, args=(srckey, destfile)) #.start()
    rsthreads.append(t)

for x in rsthreads:
        x.start()

for y in rsthreads:
        y.join()


for key in dicDownloadInputDataS3:
        wcsize = dicDownloadInputDataS3[key]
        logfile.write(str(datetime.datetime.now()) + '\t' + "Downloading " + str(key) +
                      ", InputData, " + str(wcsize) + " from S3\n")
        srckey = s3_working_directory + '/' + wcsize + '/Input_data/' + \
                  str(key) + '_Input_data.zip'
        destfile = landing_directory + '\\' + 'Input_data\\' + \
                  str(key) + '_Input_data.zip'
        t = threading.Thread(target=downloadfilefroms3, args=(srckey, destfile)) #.start()
        idthreads.append(t)

for a in idthreads:
        a.start()

for b in idthreads:
        b.join()

logfile.write(str(datetime.datetime.now()) + '\t' + "Finished Downloading Files.")
logfile.close()
print "Finished"