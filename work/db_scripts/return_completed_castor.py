import boto3
import os
import mysql.connector
from mysql.connector import errorcode

def get_ids(table):
    # Retrieves the IDs
    dynamo_client = boto3.client('dynamodb')
    objects = dynamo_client.scan(
        TableName=table
    )

    ids = []
    for item in objects['Items']:
        ids.append(item["SubjectID"]["S"]) 
        
    return ids


def rds_connect(database, hostname):
    """Connect to an RDS database given a host name

    Args:
        database ([string]): [Name of database to connect to]
        hostname ([string]): [Hostname of database to connecto]

    Returns:
        [mysql.connector.connect]: [This is the mysql connection instance ]
    """
    try:
        db = mysql.connector.connect(
            host=str(hostname),
            user="arumaldo",
            password="Texas512",
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


def update_clinical_status():
    database = "smdtest"
    hostname = "smdtest-pub.c2nctm4nzmnu.us-east-1.rds.amazonaws.com"

    try:
        cnx = rds_connect(database, hostname)
        cur = cnx.cursor(buffered=True)
        query = f'SELECT * FROM smdtest.ePOC_EOS'
        cur.execute(query)

        subject_completion_status = {}
        for value in cur:
            subject_completion_status[value[3]] = value[5]
        return subject_completion_status
    except Exception as e:
        print(f'There was an error connecting to castor:\n{e}')
        return 0

def completed_in_castor():
    EPOC_TRUTH_TABLE = "EnrichmentEPOCTruthTable"
    subject_EOS_status = update_clinical_status()
    id_data = get_ids("EnrichmentEPOCMetadata")

    completed_subjects = []
    for subject_id_wound in id_data:
        # Check to see if the subject has completed clinical: If true update locally and on EnrichmentEPOCMetadata
        base_subject_id = subject_id_wound.split('_')[0]  # [007-001, 02]
        if base_subject_id in subject_EOS_status and subject_id_wound not in completed_subjects:
            completed_subjects.append(subject_id_wound)
    
    return completed_subjects

def update_entry(table_name, key, field, new_val):
    # Usage: update_entry(TableName, A dict of the key, the field to be changed, the value of the field)
    # Example: Changing the value of ImageCollectionDate for a Database where the primary key is Entry_ID
    # update_entry(tablename, {'Entry_ID': job['Entry_ID']}, 'ImageCollectionDate', image_collection_date)
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        response = table.update_item(
            Key=key,
            UpdateExpression=f'set {field} = :r',
            ExpressionAttributeValues={
                ':r': new_val
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(f'There was an issue updating {field} for {table_name} with {new_val} \n{e}')
        return 1

    return response


# Scipt Usage

# Return the list of subject and wound, for subjects that are marked as completed in castor and have images.
print(completed_in_castor())

# Update subject and wound "02-1024_2" with True because the biopsy images exists
# update_entry(EnrichmentEPOCMetadata, {'SubjectID': "02-1024_2"}, 'BiopsyImage', "True")\


