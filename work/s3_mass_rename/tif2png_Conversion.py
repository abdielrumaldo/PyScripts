#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:26:42 2019

@author: humbertotalavera
"""
import boto3 
from PIL import Image
import io
import os
import pandas as pd



s3 = boto3.resource('s3')
client = boto3.client('s3')
my_bucket = s3.Bucket('wakeforest')
bucket_name='wakeforest'
sb='PseudoColor'


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


for file in get_all_s3_objects(client, Bucket=bucket_name, Prefix=sb):
    k=file['Key']
    splits=k.split('/')
    if ((splits[-1].endswith('.tif')) and (~splits[-1].startswith('Pseudo'))):
        rd=splits[-1].replace('.0','')
        #print(name)
        name=splits[-1].replace('.tif','.png')
        file_stream = io.BytesIO()
        image=my_bucket.Object(k)
        image.download_fileobj(file_stream)
        im = Image.open(file_stream)
        im.save('/Users/humbertotalavera/Pictures/pngs/'+name, "PNG", quality=95,compress_level=1) 
        s3.meta.client.upload_file('/Users/humbertotalavera/Pictures/pngs/'+name, bucket_name, sb+'/'+name) 

       
for file in get_all_s3_objects(client, Bucket=bucket_name, Prefix=sb):
    k=file['Key']       
    splits=k.split('/')
    if ((splits[-1].endswith('.tif')) and (~splits[-1].startswith('Pseudo'))):
        #rd=splits[-1].replace('.0','')
        name=splits[-1]
        copy_source = {
        'Bucket': bucket_name,
        'Key': k
        }
        s3.meta.client.copy(copy_source, bucket_name, 'copy_ps/'+bucket_name+'_'+name)
        s3.Object(bucket_name, k).delete()




dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('ePOC_Study')
response = table.scan()
img_data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    img_data.extend(response['Items'])
    
img_data_df=pd.DataFrame(img_data)
 
    
    
    
 #fixed bad S3 locations
 # CHANGE DATATRANSFER TO NEW ONES 
 # MAY BE MULTIPLE ONES
 
dataTransfer='WakeForest_ePOC_SS_06_10_2020'


img_data_df_rf=img_data_df[(img_data_df['ImageType']=='PseudoColor')  & (img_data_df['Status']==dataTransfer)]

        
#img_data_df_rf_ps=img_data_df_rf[img_data_df_rf['Status']=='new']
#img_data_df_rf['S3_Location']=img_data_df_rf['S3_Location'].str.replace('.0','')
#dficp['S3_Location']=dficp['ImageType']+'/'+dficp['ImageType']+'_'+dficp['PID'].astype(str)+'_'+dficp['SID'].astype(str)+'_'+dficp['IMCOLLID'].astype(str)+'_'+dficp['ImageName'] 

for x,d in img_data_df_rf.iterrows():
    s3=d['S3_Location']
#    splits=s3.split('/')
#    name=splits[-1]
    img=s3.replace('tif','png')
#    if len(name.split('_'))==7:
#        d['S3_Location']=str(d['ImageType'])+'/'+str(d['ImageType'])+'_'+str(d['Site'])+'_'+str(d['DeviceType'])+'_'+str(int(d['Patient']['PatientID']))+'_'+str(d['Study'])+'_'+str(d['ImageCollectionID'])+'_'+str(img)
#    else:
#        d['S3_Location']=str(d['ImageType'])+'/'+str(d['ImageType'])+'_'+str(int(d['Patient']['PatientID']))+'_'+str(d['Study'])+'_'+str(d['ImageCollectionID'])+'_'+str(img)
#    #img=d['ImageName'].replace('tif','png')
    #str(d['Site'])+'_'+str(d['DeviceType'])+'_'
    table.update_item(
    Key={'ImageID': d['ImageID'],
     'ImageCollectionID': d['ImageCollectionID']},
    UpdateExpression="SET #ts= :var1",
    ExpressionAttributeValues={
    ':var1': img
    },
    ExpressionAttributeNames={
    '#ts': 'S3_Location'
      }
    )
    
#im = Image.open('/Users/humbertotalavera/Pictures/PseudoColor_WakeForest_SS_10_28_63_PseudoColor.png')
#im.save('/Users/humbertotalavera/Pictures/PseudoColor_WakeForest_SS_10_28_63_PseudoColor.png', "PNG", quality=95,compress_level=1) 
#
#
#
#s3.meta.client.upload_file('/Users/humbertotalavera/Pictures/pngs/'+name+'.png', bucket_name, 'PseudoColor/'+name+'.png') 
#  
#
#        
        
        
        
        
        
        
        
        
        
#        \\192.168.110.252\ImageData2
 
#    s3resource.meta.client.upload_file('/tmp/TruthImage.png', 'smdtest', 'Dev/Mask/Mask_'+pseudo_name+'.png')
#    yourpath = os.getcwd()
#    for root, dirs, files in os.walk(yourpath, topdown=False):
#        for name in files:
#            print(os.path.join(root, name))
#            if os.path.splitext(os.path.join(root, name))[1].lower() == ".tiff":
#                if os.path.isfile(os.path.splitext(os.path.join(root, name))[0] + ".jpg"):
#                    print("A jpeg file already exists for %s" % name)
#                # If a jpeg is *NOT* present, create one from the tiff.
#                else:
#                    outfile = os.path.splitext(os.path.join(root, name))[0] + ".jpg"
#                    try:
#                        im = Image.open(os.path.join(root, name))
#                        print("Generating jpeg for %s" % name)
#                        im.thumbnail(im.size)
#                        im.save(outfile, "JPEG", quality=100)
#                    except Exception, e:
#                        print e
#    