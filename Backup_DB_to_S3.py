__author__ = 'dja410'

import os, math, time, sys, subprocess
from datetime import datetime
from subprocess import Popen, PIPE
import smtplib
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from __future__ import print_function

rightNow = datetime.datetime.now()
formattedTime = str(rightNow.strftime('%Y_%m%d_%H%M%S'))
formattedDay = str(rightNow.strftime('%a'))
dir = os.getcwd()
log = open(os.path.join(dir, formattedTime, '_Python_Script.log'), 'w')

try:
    import boto
except ImportError:
    p = subprocess.Popen('pip install boto')
    print('\t*** Python package boto is being installed. ***', file = log)
    p.communicate()

from boto.s3.connection import S3Connection

try:
    import filechunkio
except ImportError:
    p = subprocess.Popen('pip install filechunkio')
    print('\t*** Python package filechunkio is being installed. ***', file = log)
    p.communicate()

from filechunkio import FileChunkIO

master_start_time = time.time()
source = 'ORCL'
srcPwd = 'pwd'
schema = 'CADTEL_DATA_597'
filename = 'APEX_FROGS_TX'
bucket = 'bucket-name'
state = 'TX'
connectString = 'CADTEL_ADMIN_LOGIC6/'+srcPwd+'@'+source
schemas = ['SDE', 'CADTEL_ADMIN_LOGIC6', 'CADTEL_DATA_597']
zipthreads = []


def ensure_dir(directory, folder):
    if not os.direxists(os.path.join(directory, folder)):
        os.mkdir(os.path.join(directory, folder))


# function that takes the sqlCommand and connectString and returns the queryReslut and errorMessage (if any)
def runSqlQuery(sqlCommand, connectString):
    session = Popen(['sqlplus', '-S', connectString], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(sqlCommand)
    return session.communicate()


def createzipfile(source, destination):
    args = "C:\\Program Files\\7-Zip\\7z.exe a -t7z " + destination + " " + source + " -mx5 -mmt4"
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


print("\tReady to EXPORT " + schema + " schema from " + source + ".", file = log)
print("\tSource: " + connectString, file = log)

folders = ['DMPs', 'logs']
print("\tMaking sure the 'DMPs' and 'logs' folders exist in the same directory as this Python script.", file = log)
print("\tIf the folders do not already exist they will be created.", file = log)
for folder in folders:
    ensure_dir(dir, folder)

print("\tExecuting SQL command to create the FROGS_DATA_PUMP_DIR.", file = log)
sqlCommand = 'CREATE DIRECTORY FROGS_DATA_PUMP_DIR AS ' + os.path.join(dir, 'DMPs') + ';'
queryResult, errorMessage = runSqlQuery(sqlCommand, connectString)
if errorMessage != '':
    print("\tAn error has occured.  Error message: " + errorMessage + ".", file = log)
    print("\tThe script will now exit.", file = log)
    log.close()
    sys.exit()

sqlCommand = 'COMMIT;'
queryResult, errorMessage = runSqlQuery(sqlCommand, connectString)
if errorMessage != '':
    print("\tAn error has occured.  Error message: " + errorMessage + ".", file = log)
    print("\tThe script will now exit.", file = log)
    log.close()
    sys.exit()

print("\tThe creation of the FROGS_DATA_PUMP_DIR has been created.", file = log)

print("\tNow executing the Oracle Data Pump commands.", file = log)
for schema in schemas:
    sqlCommand = 'EXPDP USERID=' + connectString + ' DIRECTORY=FROGS_DATA_PUMP_DIR DUMPFILE=' + formattedTime + \
                 '_' + schema + '_schema.dmp flashback_time="to_timestamp(' \
                                'to_char(systimestamp,\'mm/dd/yyyy\hh24:mi:ss.ff9\'), ' \
                                '\'mm/dd/yyyy\hh24:mi:ss.ff9\')" ' \
                                'LOGFILE=' + formattedTime + '_' + schema + '_schema_export.log ' \
                                'SCHEMAS=' + schema + ' PARALLEL=12;'
    queryResult, errorMessage = runSqlQuery(sqlCommand, connectString)
    if errorMessage != '':
        print("\tAn error has occured.  Error message: " + errorMessage + ".", file = log)
        print("\tThe script will now exit.", file = log)
        log.close()
        sys.exit()

print('\tExport of ' + schema[0] + ', ' + schema[1] + ' and ' + schema[3] +
      ' schemas from ' + source + ' completed.', file = log)

print('\tDeleting the old 7z files from last ' + formattedDay + '.', file = log)
existingFiles = os.listdir(os.path.join(dir, 'DMPs'))
for file in existingFiles:
    if file.endswith('_' + filename + '_' + formattedDay + '.7z'):
        os.remove(os.path.join(dir, 'DMPs', file))


winDest = os.path.join(dir, 'DMPs', formattedTime + '_' + filename + '_' + formattedDay + '.7z')
sources = [dir + '\\' + 'DMPs' + '\\' + formattedTime + '_*' + '.log',
           dir + '\\' + 'DMPs' + '\\' + formattedTime + '_*' + '.dmp']

print("\tCombining the log and dmp files into one 7z file.", file = log)
start_time = time.time()
for source in sources:
    proc = createzipfile(source, winDest)
    zipthreads.append(proc)

try:
    while True:
        zipstatus = [p.poll() for p in zipthreads]
        if all([x is not None for x in zipstatus]):
            break
    elapsed_time = time.time() - start_time
    print('\tThe 7z file file has been created.  It took ' + elapsed_time + ' to complete.', file = log)
    print("\tThe 7z file has been created here: " + winDest, file = log)
    print("\tDeleting the raw log and dmp files.", file = log)
    existingFiles = os.listdir(os.path.join(dir, 'DMPs'))
    for file in existingFiles:
        if formattedTime in file:
            if file.lower().endswith('.dmp') or file.lower().endswith('.log'):
                os.remove(os.path.join(dir, 'DMPs', file))
except Exception, e:
    # send an email with the error message
    print('\tAn excpetion has occured.  Error Message: ' + str(e) + '.', file = log)
    print('\tThe python script will now exit PRIOR to uploading anything to S3.', file = log)
    log.close()
    sys.exit()

start_time = time.time()
print('\tBeginning upload of 7z file to our S3 bucket.', file = log)
bucket = 'vz3-backups-singapore'
key = 'FROGS_DB/DMPs/' + state + '/' + formattedTime + '_' + filename + '_' + formattedDay + '.7z'
uploadfiletos3(winDest, key, bucket)
elapsed_time = time.time() - start_time
print('\tUpload of 7z file to S3 has completed. It took ' + elapsed_time + ' to complete.', file = log)
print('\tThe 7z file can be found in bucket: ' + bucket, file = log)
print('\tAnd has this path: ' + key, file = log)


if formattedDay == 'Sun':
    print('\tAn email will now be sent notifying everyone of the successful Oracle Export and S3 Upload.', file = log)
    msg = MIMEMultipart()
    msg["Subject"] = "DB Backup for " + state + " in India on " + formattedTime
    msg["From"] = "email_addr"
    msg["To"] = "email_addr"
    msg["Cc"] = "email_addr"
    body = MIMEText("Results of the tablespace SQL will go here.")
    msg.attach(body)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login("email_addr", "pwd")
    server.sendmail(msg["From"], msg["To"].split(",") + msg["Cc"].split(","), msg.as_string())
    server.quit()

total_elapsed_time = time.time() - master_start_time
print('\tThe script has now completed. It took ' + total_elapsed_time + ' to complete.', file = log)
log.close()
