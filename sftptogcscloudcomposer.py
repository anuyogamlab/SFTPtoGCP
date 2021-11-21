import airflow
from airflow.operators import bash_operator
import pysftp
import datetime
from airflow.operators import python_operator
import subprocess
#from airflow.operators.bash_operator import BashOperator
 
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
# Step 1: Pass SFTP server access details
myHostname = ""
myUsername = ""
myPassword = ""
sftpDirectory=""
gcsStagingPath=""
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
 
default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}
 
with airflow.DAG(
        'sftp_2_gcs',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:
    def sftpExample():
        with pysftp.Connection(myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
            #Checks current directory to land files from SFTP temporarily
            out=subprocess.check_output(["pwd"])
            out=out.decode('utf-8').rstrip('\n')
            temp=out+"/test" 
            # Creates a test directory
            directory="mkdir -p "+temp
            subprocess.call(directory,shell=True)
            clean="rm -r "+temp+"/*"
            # Clean up the temp directory
            subprocess.call(clean,shell=True)
            # Copy files from SFTP to local
            sftp.get_r(sftpDirectory,temp)
            sftp.close()
            out1=subprocess.call("gsutil -m cp -r "+ temp +"/* "+gcsStagingPath,shell=True)
            # Clean up the temp directory
            subprocess.call(clean,shell=True) 
    # Print the dag_run id from the Airflow logs
    print_dag_run_conf = python_operator.PythonOperator(task_id='celestica',python_callable=sftpExample)
