import base64
import pysftp
import subprocess
import pysftp
import os
from google.cloud import storage
import shutil
from datetime import datetime
 
def sftptogcp(event, context):
currentBatch = datetime.today().replace(day=1).strftime('%Y%m%d')
myHostname = "<Your Host Name>"
myUsername = "<Your User Name>"
myPassword = "<Your Password>"
sftpPath = "<Path to the Folders/Files>"
sftpDirectory = "<Enter Directory>"
localTemp = "/tmp/"+currentBatch+"/"
# Enter your bucket name
gcsBucket = "stfptogcs"
gcsFolder = "composer/"+currentBatch+"/"
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
pubsub_message = base64.b64decode(event['data']).decode('utf-8')

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
   print("Connection succesfully stablished ... ")
   # /tmp is a locally writable temporary directory
   temp="/tmp"
   print(os.listdir(temp))
   print("Building a temporary directory ... ")
   sftp.cwd(sftpPath)
   sftp.get_r(currentBatch,temp)
   print(os.listdir(temp))
   sftp.cwd(sftpPath+"/"+sftpDirectory)
   # Obtain structure of the remote directory '/var/www/vhosts'
   print("Listing directory in /tmp ...")
   directory_structure = sftp.listdir_attr()
   storage_client = storage.Client()
   # Print data
   print("Writing the SFTP files to GCS bucket ... ")
   for attr in directory_structure:
     print(attr.filename, attr)
     bucket = storage_client.get_bucket(gcsBucket)
     blob = bucket.blob(gcsFolder+attr.filename)
     blob.upload_from_filename(localTemp+attr.filename)
     print('File {} uploaded to {}.'.format(attr.filename,gcsBucket+gcsFolder))
   sftp.close()
