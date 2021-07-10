import boto3
import re
import os
import mysql.connector
from mysql.connector import errorcode

#Static
EVENT_DATA = { "version": "0",  "id": "ffb1dee4-439a-e60d-5954-776e47d23129",  "detail-type": "DataSync Task Execution State Change",  "source": "aws.datasync",  "account": "921329379223",  "time": "2021-06-04T18:54:32Z",  "region": "us-east-1",  "resources": [    "arn:aws:datasync:us-east-1:921329379223:task/task-070ae55e8e777c356/execution/exec-0710e3b76849604f1"  ],  "detail": {    "State": "SUCCESS"  }}

# Configurations
sns_client = boto3.client('sns')
datasync_client = boto3.client('datasync')
cloudwatch_client = boto3.client('logs')
s3 = boto3.resource('s3')


def rds_connect():
    """Connect to an RDS database given a host name

    Args:
        database ([string]): [Name of database to connect to]
        hostname ([string]): [Hostname of database to connecto]

    Returns:
        [mysql.connector.connect]: [This is the mysql connection instance ]
    """

    database = "smdtest"
    # hostname = "smdtest2.c2nctm4nzmnu.us-east-1.rds.amazonaws.com"
    hostname = "smdtest-pub.c2nctm4nzmnu.us-east-1.rds.amazonaws.com"

    try:
        db = mysql.connector.connect(
            host=str(hostname),
            user="htalavera",
            password="Texas512!",
            database=str(database),
            port=3308
        )
    except mysql.connector.Error as err:

        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print(f'Something is wrong with your username {os.getenv("SQLUSER")} or password {os.getenv("SQLPASS")}')
            return False
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            return False
        else:
            print(err)
            return False
    else:
        print(f'Connecting to {database}')
        return db

def exec_sql_file(sql_file, data_transfer_name):
    """
    Expeting: sql_data = s3.Object(bucket_name, key_value)
    """
    print('Connecting to DB')
    cnx = rds_connect()
    cur = cnx.cursor(buffered=True)
    
    # Get the study 
    re_match = re.search('([_]\w+[_])', data_transfer_name).group(0)
    study = re_match.strip('_').lower()

    # Get the bucket
    re_match = re.search('([^_]\w+?[_])', data_transfer_name).group(0)
    bucket_name = re_match.strip('_').lower()

    if str.lower(bucket_name) == 'wakeforest':
        database_name = "epoc_wf_ss"
    else:
        database_name = f'{study}_{bucket_name.lower()}_ss'

    cur.execute(f'CREATE SCHEMA {database_name}')
    cur.execute('SET FOREIGN_KEY_CHECKS=0')
    cur.execute(f'USE {database_name}')

    statement = ""

    for line in sql_file.get()['Body']._raw_stream:
        line = line.decode("utf-8")

        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if re.match(r'[/*]', line):
            continue

        if not re.search(r';', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "\n\n[DEBUG] Executing SQL statement:\n%s" % (statement)
            try:
                # print(f'Executing statement: {repr(statement)[1:-1]}')
                cur.execute(statement)
            except Exception as e:
                print(statement)
                print(f'\n[WARN] MySQLError during execute statement \n\tArgs: {e}')

            statement = ""

def lambda_handler(event, context):
    print(event)
    # This is the mapping used to automate tasks from Local -> S3 -> Glacier
    # Key: Local to S3 Task
    # Value: S3 to Glacier Task
    s3_to_glacier_task_map = {
        "task-070ae55e8e777c356":"task-020f40cfb01e51283", # AWCM
        "task-083f1092f7d101692":"task-01b5fcac259f1244d", # DEERSFP
        "task-07103a066e8726de3":"task-0b1c298ebaec9e01f", # OCER
        "task-09963a66df0b29859":"task-04a631f59968bcf34", # OMWA
        "task-0827c53931aea0798":"task-0edb9d6a186143226" #WHFA
    }
    
    if event:
        # arn:aws:datasync:us-east-2:111222333444:task/task-08de6e6697796f026/execution/exec-04ce9d516d69bd52f
        task_execution_arn = event['resources'][0]
        
        # exec-04ce9d516d69bd52f
        execution_id = task_execution_arn.split('/')[-1]
        
        # arn:aws:datasync:us-east-2:111222333444:task/task-08de6e6697796f026
        task_arn = '/'.join(task_execution_arn.split('/')[0:2])
        
        # task-08de6e6697796f026 
        task_id = task_arn.split('/')[1]

        
        if task_id in s3_to_glacier_task_map.keys():
            # This means that this transfer is local to S3
            print("The data transfer is from Local to s3")
                
            try:
                task_data = datasync_client.describe_task(
                    TaskArn = task_arn
                    )
            except Exception as e:
                print(f'ERROR Getting the task failed due to the following:\n{e}')

            try:
                location_data = datasync_client.describe_location_s3(
                    LocationArn = task_data['DestinationLocationArn']
                    )
            except Exception as e:
                print(f'ERROR Getting the location data failed due to the following:\n{e}')
            
            try:
                log_data = cloudwatch_client.get_log_events(
                    logGroupName="/aws/datasync",
                    logStreamName=f'{task_id}-{execution_id}',
                    limit=10,
                    startFromHead=False
                )
            except Exception as e:
                print(f'ERROR Getting the Cloudwatch logs  failed due to the following:\n{e}')


            bucket_base_name = location_data['LocationUri'].split("//")[1]
            bucket_name = bucket_base_name.split('/')[0]

            for log in log_data['events']:
                print(log['message'])

                result = re.search(f'({str.upper(bucket_name)}\w+-\w+-\w+-\w+-\w+)', log['message'])
                
                if result.group(0):
                    print(f'FOUND: {result.group(0)}')
                    data_transfer_name = result.group(0)
                    break
                
            
            key_value = f'DataTransfer/{data_transfer_name}/SpectralView/SoftwareData.sql'
            sql_data = s3.Object(bucket_name, key_value)

            exec_sql_file(sql_data, data_transfer_name)
            


if __name__ == "__main__":
    lambda_handler(EVENT_DATA, "")