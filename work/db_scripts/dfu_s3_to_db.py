#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 16:27:14 2019

update target tables with software.sql

change variable SB and BucketName

@author: humbertotalavera
"""


import boto3
from urllib.parse import urlparse
import pandas as pd 
import uuid
import datetime as dt
import re

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.sql import text

# Configurations
sns_client = boto3.client('sns')
datasync_client = boto3.client('datasync')
cloudwatch_client = boto3.client('logs')
s3 = boto3.resource('s3')



def ImageType(row):
    if 'Assessing' in row['ImageName']:
        return 'Assessing'
    elif 'Mask' in row['ImageName']:
        return 'Mask'
    elif 'Truth' in row['ImageName']:
        return 'Truth'
    elif row['ImageName'].startswith(('0_000','1_000','2_000','3_000','4_000','5_000','6_000','7_000_F855')):
        return 'MSI' 
    elif row['ImageName'].startswith(('0_','1_','2_','3_')):
        return 'Raw' 
    elif 'raw' in row['ImageName']:
        return 'Raw' 
    elif 'white-balanced' in row['ImageName']:
        return 'white-balanced' 
    elif 'flatfield-corrected' in row['ImageName']:
        return 'flatfield-corrected' 
    elif 'flatfield-corrected' in row['ImageName']:
        return 'flatfield-corrected' 
    elif 'PseudoColor' in row['ImageName']:
        return 'PseudoColor' 
    elif 'Deepview' in row['ImageName']:
        return 'Deepview' 
    elif 'Reference' in row['ImageName']:
        return 'Reference' 
    else:
        return 'Raw'

def ImageColl(row):
    sql=text(f'Select IMCOLLID,{db_table}.imagescollection.Ignore from {db_table}.imagescollection where ImageCollFolderName= :d1')
    result=con.execute(sql,d1=row['ImgCollName'])
    if result.rowcount>1:
        return 'NULL'
    else:
        ic=result.first()
        row.loc[['IMCOLLID','Ignore']]=[ic[0],ic[1]]
        return row

def Patient(row):
    sql=text(f'Select * from {db_table}.patient where PID= :d1')
    result=con.execute(sql,d1=row['PID'])
    if result.rowcount>1:
        return 'NULL'
    elif result.rowcount<1:
        return 'No Matches'
    else:
        ic=result.first()
        
        return row


def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')
            

def main(data_transfer_name):


       # Get the site 
    re_match = re.search('([_]\w+[_])', data_transfer_name).group(0)
    study = re_match.strip('_').lower()

    # Get the bucket
    re_match = re.search('([^_]\w+?[_])', data_transfer_name).group(0)
    bucket_name = re_match.strip('_').lower()

    if str.lower(bucket_name) == 'wakeforest':
        db_table = "epoc_wf_ss"
    else:
        db_table = f'{study}_{bucket_name.lower()}_ss'
    
    print(f'Connecting to Database: {db_table}')
    #DEFINE VARIABLES
    ##Assign Parameters
    sb=f'DataTransfer/{data_transfer_name}' # specific bucket to parse

    dynamo_table='DFU_Study_DEV'
    device_type='SS'
    studyName=study.upper()
    site=bucket_name.upper()
    status=data_transfer_name
    

    ##Retrieve Bucket Objects
    s3 = boto3.resource('s3')
    client = boto3.client('s3')
    my_bucket = s3.Bucket(bucket_name)

 
    Base = automap_base()
    engine = create_engine(f'mysql+mysqlconnector://htalavera:Texas512!@smdtest2.c2nctm4nzmnu.us-east-1.rds.amazonaws.com:3308/{db_table}', echo=False)

    Base.prepare(engine, reflect=True)
    session = Session(engine)
    con=engine.connect()

    df=pd.DataFrame()


    for file in get_all_s3_objects(client, Bucket=bucket_name, Prefix=sb):
        k=file['Key']
        
        meta=client.head_object(Bucket=bucket_name,Key=k)
        k = k.replace("DataTransfer/", '')
        print(k)
        splits=k.split('/')
        if ((splits[-1].endswith(('.tiff','.tif','png','jpg'))) and ('CapImages' not in k)):
            p=splits[2]
            mode=splits[3]
            s=splits[4]
            a=splits[5]
            n=str(splits[6])
            m=splits[7]
            images=splits[8]
            path=k
            ImageDate=meta['ResponseMetadata']['HTTPHeaders']['last-modified']
            df = df.append({'Patient': p, 'Study': s, 'Anatomical': a, 'Series': n, 'Anatomical': a, 'ImageCollection': m, 
                            'ImageName': images, 'Module':mode,'FileName':path,'ImageDate':ImageDate,
                            'Device':'PE1','Site':site,'UUID':uuid.uuid4()}, ignore_index=True)
    
    #o = urlparse('s3://bucket_name/folder1/folder2/file1.json')

    #df.to_csv('/Users/humbertotalavera/'+sb+'_'+device_type+'.csv')
    #df=df.from_csv('/Users/humbertotalavera/'+sb+'_'+device_type+'.csv')

    df['Frequency']=df['ImageName'].str.split('_').str[2]
    df['PID']=df.Patient.str.extract('(\d+)')
    print(df)
    df['SID']=df.Study.str.extract('(\d+)')
    #df['ImageDate_Formatted']=pd.to_datetime(df['ImageDate'], unit='s')
    df['ImageType']=''
    df['ImageType'] = df.apply(ImageType, axis=1)
    df['newPath']=df['ImageType']+'/'+df['ImageType']+'_'+df['ImageName'] 
    df['Series']=df['Series'].astype(str)
    #df['FileName']=df['FileName'].str.replace('/disk4/WakeForest_poc_GMP1_01_23_2018/','/disk4/WakeForest_poc_GMP1_12_13_2017/SpectralView/')
    df['ImgCollName']='D:\\SpectralView\\'+df['Patient'].astype(str)+'\\'+df['Module']+'\\'+df['Study'].astype(str)+'\\'+df['Anatomical'].astype(str)+'\\'+df['Series'].astype(str)+'\\'+df['ImageCollection'].astype(str)+'\\'
    sql=text(f'Select IMCOLLID,ImageCollFolderName,{db_table}.imagescollection.Ignore from {db_table}.imagescollection')
    result=con.execute(sql)
    x=result.fetchall()
    ic=pd.DataFrame(x)
    ic.columns=['IMCOLLID','FolderName','Ignore']
    df2=df   
    dfic=df.merge(ic,how='left',left_on='ImgCollName',right_on='FolderName')

    psql=text(f'Select PID,MedicalNumber, Sex, Birthday FROM {db_table}.patient')
    pr=con.execute(psql)
    px=pr.fetchall()
    p=pd.DataFrame(px)
    p.columns=['PID','MedicalNumber','Gender','Birthday']
    p['PID']=p['PID']
    print(dfic)
    dfic['PID']=dfic['PID'].astype(int)
    p['MedicalNumber']=p['MedicalNumber'].astype(str)
    dficp=pd.merge(dfic,p,how='left',on='PID')

    #dficp.to_csv('/Users/humbertotalavera/'+sb+'_'+device_type+'dficp.csv')

    #dficp['Ethnicity']='Null'
    #dficp['Race']='Null'
    #dficp['Mechanism']='Null'
    #dficp['TBSA']='Null'
    #dficp['Ethnicity']='Null'
    #dficp['Birthday']='Null'
    dficp['S3_Location']=dficp['ImageType']+'/'+dficp['ImageType']+'_'+site+'_'+device_type+'_'+dficp['PID'].astype(str)+'_'+dficp['SID'].astype(str)+'_'+dficp['IMCOLLID'].astype(str)+'_'+dficp['ImageName'] 
    now=dt.datetime.now()
    dficp['UploadDate']=now.strftime("%Y-%m-%d %H:%M:%S")

    #finaldf=dficp[dficp['ImageType']!='PPG']
    finaldf=dficp[~(dficp['IMCOLLID'].isnull())]
    #finaldf=dficp[(dficp['ImageType']=='Reference') | (dficp['ImageType']=='Raw') ]
    finaldf['Bucket']=bucket_name
    finaldf['Ignore']=1
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(dynamo_table)


    for i,r in finaldf.iterrows():
        table.put_item(Item={'ImageID':str(r['UUID']),'ImageCollectionID':int(r['IMCOLLID']),
                                                        'Patient':{
                                                                'PatientID':int(r['PID']),
                                                                'Gender':str(r['Gender']),
                                                                'Birthday':int(r['Birthday'])
                                                                },
                                                                'Anatomical':str(r['Anatomical']),'S3_Location':str(r['S3_Location']),\
                            'Burn':str(r['Series']),'ImageType':str(r['ImageType']),'ImageName':str(r['ImageName']),'FolderName':str(r['FolderName']),\
                            'ImgCollName':str(r['ImgCollName']),'Site':str(r['Site']),'DeviceType':str(device_type),'Frequency':str(r['Frequency']),\
                            'Device':str(r['Device']),'Study':int(r['SID']),'MedicalNumber':str(r['MedicalNumber']),'UploadDate':str(r['UploadDate']),\
                            'Bucket':str(r['Bucket']),'DataTransfer':str(sb),'Status':str(status),'StudyName':str(studyName),'Ignore':int(r['Ignore'])})

    for x,d in finaldf.iterrows(): 
        copy_source = {
            'Bucket': bucket_name,
            'Key': d['FileName']
        }
        s3.meta.client.copy(copy_source, bucket_name, d['S3_Location'])
        
    #PLEASE REMEMBER TO CONVER TO TO PNG AFTER YOU RECIEVE BRIGHT IMAGES


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
            main(data_transfer_name)

#Static
EVENT_DATA = { "version": "0",  "id": "ffb1dee4-439a-e60d-5954-776e47d23129",  "detail-type": "DataSync Task Execution State Change",  "source": "aws.datasync",  "account": "921329379223",  "time": "2021-06-04T18:54:32Z",  "region": "us-east-1",  "resources": [    "arn:aws:datasync:us-east-1:921329379223:task/task-070ae55e8e777c356/execution/exec-0710e3b76849604f1"  ],  "detail": {    "State": "SUCCESS"  }}

if __name__ == "__main__":
    lambda_handler(EVENT_DATA, "")