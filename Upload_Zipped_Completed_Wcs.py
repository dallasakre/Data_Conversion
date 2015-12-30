__author__ = 'dja410'

import zipfile
import os, math
import psycopg2
import datetime
from boto.s3.connection import S3Connection
import boto
from filechunkio import FileChunkIO
import threading
import subprocess

working_directory = 'z:\\'
s3_working_directory = 'FFS/'


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


def gets3bucketlisting(connection, directory, bucket):
    bucket = connection.get_bucket(bucket, validate=False)
    keys = bucket.list(prefix=directory)
    return keys


def getSuccessfulScaled(connection):
    arrWcAndPath = []
    sql = "SELECT SUBSTR(request, INSTR(request, 'name=\"State\">', 1, 1)+13, 2) as state, "
    sql = sql + "SUBSTR(request, INSTR(request, 'name=\"Jurisdiction\">', 1, 1)+20, 2) as jur, "
    sql = sql + "SUBSTR(request, INSTR(request, 'name=\"Wire_Center\">', 1, 1)+19, 4) as wc, "
    sql = sql + "'\\\\' || engine_host || '\\d$\\ICGS\OutputFFSfiles\\' || SUBSTR(request, "
    sql = sql + "INSTR(request, 'name=\"Jurisdiction\">', 1, 1)+20, 2) || '_' || "
    sql = sql + "SUBSTR(request, INSTR(request, 'name=\"Wire_Center\">', 1, 1)+19, 4) as path "
    sql = sql + "from fme_job_history WHERE job_status = 8 AND SUBSTR(request, "
    sql = sql + "INSTR(request, '/', 1, 2)+1, INSTR(request, '.fmw', 1, 1)-INSTR(request, "
    sql = sql + "'/', 1, 2)+3) = 'FTR-ICGS2FROGS_S(BatchFiles)_2015_09_19.fmw' "
    sql = sql + "order by SUBSTR(request, INSTR(request, 'name=\"State\">', 1, 1)+13, 2);"

    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    for row in rows:
        jurwc = str(row[1]) + "_" + str(row[2])
        path = str(row[3])
        value = (jurwc, path)
        arrWcAndPath.append(value)

    return arrWcAndPath


def getSuccessfulNonscaled(connection):
    arrWcAndPath = []
    sql = "SELECT SUBSTR(request, INSTR(request, 'name=\"State\">', 1, 1)+13, 2) AS State, "
    sql = sql + "SUBSTR(request, INSTR(request, 'name=\"Jurisdiction\">', 1, 1)+20, 2) AS Jur, "
    sql = sql + "SUBSTR(request, INSTR(request, 'name=\"Wire_Center\">', 1, 1)+19, 4) AS WC, "
    sql = sql + "'\\\\' || engine_host || '\\d$\\ICGS\\OutputFFSfiles\\' || SUBSTR(request, "
    sql = sql + "INSTR(request, 'name=\"Jurisdiction\">', 1, 1)+20, 2) || '_' || SUBSTR(request, "
    sql = sql + "INSTR(request, 'name=\"Wire_Center\">', 1, 1)+19, 4) || '\\NONSCALED' as PATH "
    sql = sql + "FROM fme_job_history WHERE job_status = 8 AND SUBSTR(request, INSTR(request, "
    sql = sql + "'/', 1, 2)+1, INSTR(request, '.fmw', 1, 1)-INSTR(request, '/', 1, 2)+3) = "
    sql = sql + "'FTR-ICGS2FROGS_NS(BatchFiles)_2015-09-19.fmw' "
    sql = sql + "order by SUBSTR(request, INSTR(request, 'name=\"State\">', 1, 1)+13, 2)"

    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    for row in rows:
        jurwc = str(row[1]) + "_" + str(row[2])
        path = str(row[3])
        value = (jurwc, path)
        arrWcAndPath.append(value)

    return arrWcAndPath


def createpostgresconnection():
    try:
        connstring = "user='fmeserver' host='172.31.15.81' password='pwd' port='7082'"
        conn = psycopg2.connect(connstring)
    except:
        print 'Unable to connect to the database with connection string: "' + connstring + '".'
    return conn


def createzipfile(source, destination):
    args = "C:\\Program Files\\7-Zip\\7z.exe a -tzip " + destination + " " + source + " -mx5 -y"
    p = subprocess.Popen(args)
    return p


def uploadfiletos3(srcfile, destkey, bucket):
    source_path = srcfile
    source_size = os.stat(source_path).st_size
    s3conn = boto.s3.connect_to_region('ap-southeast-1',
                                       aws_access_key_id='key',
                                       aws_secret_access_key='secret',
                                       calling_format=boto.s3.connection.OrdinaryCallingFormat()
                                       )
    b = s3conn.get_bucket(bucket, validate=False)

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


def rightnow():
    t = str(datetime.datetime.now())
    return t


## Do the actual work
print rightnow() + '\tBeginning to Zip and Upload Completed Wirecenters!'
logfile = open(working_directory + 'LogFiles' + '\\UploadSuccessToS3_' +
               str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + '.log', 'w+')
print rightnow() + '\tCreated Logfile in the following diectory: ' + str(logfile.name)


dicExistingS3ZipFiles = {}
dicExistingFFSZipFiles = {}
newzippedfilepaths = []
ec2servers = ['Win-v335bor09f8', 'Win-v335bor09f9', 'Win-v335bor09fA', 'Win-v335bor09fB', 'Win-v335bor09fC']
s3buckets = [('apex-singapore', 'FL'), ('apex-singapore', 'TX'), ('ramtech-singapore', 'CA')]
aryWcSuccesses = []
nsaryWcSuccesses = []
zipthreads = []
upthreads = []
allffszipfiles = []

print rightnow() + '\tConnecting to FME Server Postgres DB'
fmeconn = createpostgresconnection()
print rightnow() + '\tGathering all successfully processed SCALED and NONSCALED WCs from FME Server Postgres DB'
aryWcSuccesses = getSuccessfulScaled(fmeconn)
nsaryWcSuccesses = getSuccessfulNonscaled(fmeconn)
for x, y in nsaryWcSuccesses:
    aryWcSuccesses.append((x,y))


## Add logic to read through directories to see what is already zipped
print rightnow() + '\tReading through EC2 directories to determine what data was previously zipped'
for server in ec2servers:
    path = '\\d$\\ICGS\\OutputFFSfiles\\ZippedFFSFiles'
    for file in os.listdir('\\\\' + server + path):
        if file.endswith('.zip'):
            try:
                dicExistingFFSZipFiles[file] = server
            except:
                print "Error!  More than 1 Zip file named '" + file + \
                      "' exists.  Servers include but are not limited to: '" + \
                      server + "' and '" + str(dicExistingFFSZipFiles[file]) + "'.\n"


## Add logic to zip directories listed in aryWcSuccess and not already zipped, put these zipped files in ZippedFFSFiles
print rightnow() + '\tCreating zip files for successful SCALED and NONSCALED data'
for jurwc, path in aryWcSuccesses:
        source = path + "\\*.ffs"
        if 'NONSCALED' in path:
            fileKey = jurwc + "_NONSCALED.zip"
            if fileKey not in dicExistingFFSZipFiles:
                destination = str(path.split(jurwc)[0]) + "ZippedFFSFiles\\" + fileKey
            else:
                continue
        else:
            fileKey = jurwc + "_SCALED.zip"
            if fileKey not in dicExistingFFSZipFiles:
                destination = str(path.split(jurwc)[0]) + "ZippedFFSFiles\\" + fileKey
            else:
                continue
        proc = createzipfile(source, destination)
        zipthreads.append(proc)


while True:
    zipstatus = [p.poll() for p in zipthreads]
    if all([x is not None for x in zipstatus]):
        break

## Get final list of all zip files including new and old
print rightnow() + '\tReading through EC2 directories to get final list of all zip files including new and old'
for server in ec2servers:
    path = '\\d$\\ICGS\\OutputFFSfiles\\ZippedFFSFiles'
    for file in os.listdir('\\\\' + server + path):
        if file.endswith('.zip'):
            directory = '\\\\' + server + path + '\\' + file
            try:
                allffszipfiles.append((file, directory))
            except:
                print "Error!  Could not add file: '" + file + "' to array."


## Create connection to S3 to query keys in buckets
print rightnow() + '\tConnecting to S3'
s3conn = boto.s3.connect_to_region('ap-southeast-1',
                                   aws_access_key_id='key',
                                   aws_secret_access_key='secret',
                                   calling_format=boto.s3.connection.OrdinaryCallingFormat()
                                   )


## Add logic to figure out what Zip files already exist on S3
print rightnow() + '\tReading through APEX and Ramtech buckets to create list of already uploaded zip files'
for bucket, state in s3buckets:
    try:
        path = 'FFS/' + state + '/ready_for_processing/'
        logfile.write(str(datetime.datetime.now()) + '\t' +
                      'Creating Dictionary of ' + bucket + ' ' + state +
                      ' S3 Wirecenters in Directory: ' + path + '\n')
        rsExistingFFSZipFiles = gets3bucketlisting(s3conn, path, bucket)
        for item in rsExistingFFSZipFiles:
            try:
                if not item.name == '':
                    dicExistingS3ZipFiles[item.name] = 'Exists'
            except:
                continue
    except Exception, e:
        logfile.write(str(datetime.datetime.now()) + '\t' + 'An error occurred: ' + str(e) + '\n')


## Add logic to upload zipped files to appropriate S3 bucket
print rightnow() + '\tUploading new zip files to S3'
for filekey, ec2directory in allffszipfiles:
    if filekey[0] == 'C' or filekey[0] == '1':
        destkey = 'FFS/CA/ready_for_processing/' + filekey
        bucket = 'ramtech-singapore'
    elif filekey[0] == 'F':
        destkey = 'FFS/FL/ready_for_processing/' + filekey
        bucket = 'apex-singapore'
    elif filekey[0] == 'T' or filekey[0] == '2':
        destkey = 'FFS/TX/ready_for_processing/' + filekey
        bucket = 'apex-singapore'
    if destkey not in dicExistingS3ZipFiles:
        source_size = os.stat(ec2directory).st_size
        if not source_size == 0:
            print rightnow() + '\tUploading new zip files to S3 from the following EC2 directory: ' + ec2directory
            t = threading.Thread(target=uploadfiletos3, args=(ec2directory, destkey, bucket))
            upthreads.append(t)
        else:
            print rightnow() + '\tA file with size 0 kb failed to upload to S3 "' + deskey + \
                  "' please address this issue by deleting the file and re-uploading or ignore " + \
                  "this message and know that the file will not be uploaded to S3."

for a in upthreads:
    a.start()

for b in upthreads:
    b.join()


logfile.write(str(datetime.datetime.now()) + '\t' + "Finished Uploading new zip files to S3.")
logfile.close()
print rightnow() + "\tFinished Uploading new zip files to S3!"
print rightnow() + "\tProcessing Complete."