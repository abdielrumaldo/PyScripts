#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 15:11:12 2020
update mask 
@author: humbertotalavera
"""

import pandas as pd
import boto3
import json
import base64
import random as rd
import numpy as np
from PIL import Image
import os
import io
from datetime import datetime as dt

truthingDate = dt.strptime('06/10/2020', "%m/%d/%Y") 
smClient = boto3.client('sagemaker')

s3resource= boto3.resource('s3')
s3Client= boto3.client('s3')
bucket='wakeforest'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
smd_images = dynamodb.Table('ePOC_Study')
smd_response = smd_images.scan()
smd_data = smd_response['Items']

while 'LastEvaluatedKey' in smd_response:
    smd_response = smd_images.scan(ExclusiveStartKey=smd_response['LastEvaluatedKey'])
    smd_data.extend(smd_response['Items'])

smd_images_df=pd.DataFrame(smd_data)

def to_number(s):
    try:
        s1 = float(s)
        return s1
    except ValueError:
        return s

# def OverlayMask(Mask, PseudoImage, output_shape=(None,None)):
    
#     OG_colorKeyGT = {'Background': [0, 0, 0],
#        'Viable': [153, 102, 51],
#        'First_Degree': [255, 174, 201],
#        'Shallow_Second_Degree':[183, 179, 0],
#        'Deep_Second_Degree': [255, 127, 39],
#        'Third_Degree': [128, 128, 128],
#        'Silvadene': [164, 149, 215],
#        'Woundbed_Epinephrine': [237, 28, 36],
#        'Woundbed_Donor_Site': [163, 73, 164],
#        'Unknown_Category':[19,255,0],
#        'other': [255, 255, 255]}
    
#     SageMaker_colorKeyGT = {'Background': [188, 189, 34],
#        'Viable': [148, 103, 189],
#        'First_Degree': [44, 160, 44],
#        'Shallow_Second_Degree':[31, 119, 180],
#        'Deep_Second_Degree': [255, 127, 14],
#        'Third_Degree': [214, 39, 40],
#        'Silvadene': [127, 127, 127],
#        'Woundbed_Epinephrine': [140, 86, 75],
#        'Woundbed_Donor_Site': [227, 119, 194],
#        'Unknown_Category':[19,255,0],
#        'other': [255, 152, 150]}
    
#     r = Mask[:,:,0]
#     g = Mask[:,:,1]
#     b = Mask[:,:,2]
     
#     label = np.zeros((r.shape[0], r.shape[1], 3), dtype=np.float64)
    
#     for key in OG_colorKeyGT:
#         if key in SageMaker_colorKeyGT.keys():
#             index1 = r == SageMaker_colorKeyGT[key][0]
#             index2 = g == SageMaker_colorKeyGT[key][1]
#             index3 = b == SageMaker_colorKeyGT[key][2]
        
#             label[index1, 0] = OG_colorKeyGT[key][0]
#             label[index2, 1] = OG_colorKeyGT[key][1]
#             label[index3, 2] = OG_colorKeyGT[key][2]
            
#             PseudoImage[index1, 0] = OG_colorKeyGT[key][0]
#             PseudoImage[index2, 1] = OG_colorKeyGT[key][1]
#             PseudoImage[index3, 2] = OG_colorKeyGT[key][2]
#     PseudoImage = np.array(PseudoImage,dtype=np.uint8)
#     label = np.array(label,dtype=np.uint8)
#     return PseudoImage, label


# change 1 modify OverlayMask func
def OverlayMask(Mask, PseudoImage, output_shape=(None,None)):
    
    OG_colorKeyGT = {'Background': [0, 0, 0],
       'Viable': [153, 102, 51],
       'First_Degree': [255, 174, 201],
       'Shallow_Second_Degree':[183, 179, 0],
       'Deep_Second_Degree': [255, 127, 39],
       'Third_Degree': [128, 128, 128],
       'Silvadene': [164, 149, 215],
       'Woundbed_Epinephrine': [237, 28, 36],
       'Woundbed_Donor_Site': [163, 73, 164],
       'Unknown_Category':[19,255,0],
       'other': [255, 255, 255]}
    
    SageMaker_colorKeyGT = {'Background': [188, 189, 34],
       'Viable': [148, 103, 189],
       'First_Degree': [44, 160, 44],
       'Shallow_Second_Degree':[31, 119, 180],
       'Deep_Second_Degree': [255, 127, 14],
       'Third_Degree': [214, 39, 40],
       'Silvadene': [127, 127, 127],
       'Woundbed_Epinephrine': [140, 86, 75],
       'Woundbed_Donor_Site': [227, 119, 194],
       'Unknown_Category':[19,255,0],
       'other': [255, 152, 150]}
    
    
    r = Mask[:,:,0]
    g = Mask[:,:,1]
    b = Mask[:,:,2]
    label = np.zeros((r.shape[0], r.shape[1], 3), dtype=np.float64)
    
    for key in SageMaker_colorKeyGT.keys():
        # find index1 & index2 & index3 overlay part
        index1 = (r == SageMaker_colorKeyGT[key][0])*1
        index2 = (g == SageMaker_colorKeyGT[key][1])*1
        index3 = (b == SageMaker_colorKeyGT[key][2])*1
        index = index1 + index2 + index3
        final_index = index == 3
            
        label[final_index, 0] = OG_colorKeyGT[key][0]
        label[final_index, 1] = OG_colorKeyGT[key][1]
        label[final_index, 2] = OG_colorKeyGT[key][2]

        PseudoImage[final_index, 0] = OG_colorKeyGT[key][0]
        PseudoImage[final_index, 1] = OG_colorKeyGT[key][1]
        PseudoImage[final_index, 2] = OG_colorKeyGT[key][2]
    PseudoImage = np.array(PseudoImage,dtype=np.uint8)
    label = np.array(label,dtype=np.uint8)
    return PseudoImage, label

# change 2 find all the .json files from bucket 

bucket_name = s3resource.Bucket("wakeforest")
sb="sagetest/output/"
# df=pd.DataFrame()
def get_all_s3_objects(mybucket, bucket_prefix):
    objs = mybucket.objects.filter(Prefix = bucket_prefix)
    # return all the .json paths which include 'annotations/worker-response/iteration-1'
    return [obj.key for obj in objs if 'annotations/worker-response/iteration-1' in obj.key]


# change 3 add for loop, but didn't change the file name and saving path when saving to local, you can change it or not
# mask='sagetest/output/nola-Subject03-1011Burn2ID186100/annotations/worker-response/iteration-1/9/2020-04-05_14:20:40.json'
for mask in get_all_s3_objects(bucket_name, sb):

    try:
        # name=mask.split('/') [2]
        # iteration=[int(s) for s in mask.split('/') if s.isdigit()]
        # mask_json=s3Client.get_object(Bucket=bucket,Key=mask)
        # stream=mask_json['Body']
        # annotation_data=stream.read()
        # ad=annotation_data.decode('utf-8')
        # ad_json=json.loads(ad)
        # answers=ad_json['answers'][0]
        # t = answers['answerContent']['crowd-semantic-segmentation']
        # imgData = t['labeledImage']['pngImageData']
        # img = base64.b64decode(imgData)
        
        
        # with open('C:/tmpC:/tmp_mask.png', 'wb') as f:
        #     f.write(img)
        # change 5 Check if img  is empty or not, not use img != 0
        if np.sum(io.imread('C:/tmpC:/tmp_mask.png')) != 0:
            pkey=f"""sagetest/output/wakeforest-Subject02-1040Burn02ID395658/annotations/worker-response/iteration-1/0/2020-10-29_20:56:13.json"""
            pseudo=s3Client.get_object(Bucket=bucket,Key=pkey)
            pseudo_stream=pseudo['Body']
            pseudo_data=pseudo_stream.read()
            pd=pseudo_data.decode('utf-8')
            lines=pd.split('\n')
            lines.remove('')
            pseudo_json={}
            for line in lines:
                row=line.split('\t')
                pseudo_json.update({row[0]:row[1]})
            uri=pseudo_json[str(iteration[0])]
            pseudo_name = uri.split('/')[-1]
            pname=pseudo_name.split('.')[0]
            pint=[int(s) for s in pname.split('_') if s.isdigit()]
            study=pint[1]
            # metadata=smd_images_df[smd_images_df['Study']==int(study)]
            # meta_pseudo=metadata[metadata['S3_Location']=='PseudoColor/'+pseudo_name]
            # meta=meta_pseudo.iloc[[0]]
            # meta['ImageID']=str(rd.randint(1, 5000000))
            # meta['ImageType']='Truth'
            # meta['S3_Location']='Truth/Truth_'+pname+'.png'
            pseudocolor = '/' + '/'.join( uri.split('/')[3:])
            nd='C:/tmp/'+str(name)
            if not os.path.exists(nd):
                os.makedirs(nd)
            s3resource.Bucket(bucket).download_file(pseudocolor[1:], nd+'/'+pseudo_name)
            PseudoImage =nd+'/'+pseudo_name
            Mask = 'C:/tmp_mask.png'
            # change 4: I use io.imread to load the images as setflags raise an error for me.
            # from skimage import io
            # PI = io.imread(PseudoImage)[:,:,:3]
            # MI = io.imread(Mask)[:,:,:3]
        
            PI = np.asarray(Image.open(PseudoImage))
            PI.setflags(write=1)
            MI = Image.open(Mask)
            MI = np.asarray(MI.convert('RGB'))
        
            #PI = cv2.imread(PseudoImage)[:,:,:3]
            #MI = cv2.imread(Mask)[:,:,:3]
        
            TruthImage, Mask = OverlayMask(MI, PI,output_shape = (480,360))
        
            #Mask = cv2.cvtColor(Mask, cv2.COLOR_BGR2RGB)
            #cv2.imwrite('Mask.png',Mask)
            #cv2.imwrite('TruthImage.png',TruthImage)
            TruthImage = Image.fromarray(TruthImage)
            TruthImage.save(nd+'/Truth_'+pseudo_name)
            TruthImage.save('C:/tmp/TruthImage.png')
            Mask = Image.fromarray(Mask)
            Mask.save(nd+'/Mask_'+pseudo_name)
            s3resource.meta.client.upload_file('C:/tmp/TruthImage.png', bucket, 'Truth/Truth_'+pname+'.png')
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.Table('ePOC_Study')
            
            # meta[['DataTransfer','Ignore','ImageCollectionID','Status','Study']]=meta[['DataTransfer','Ignore','ImageCollectionID','Status','Study']].astype(str)
            # converted = meta.convert_objects(convert_numeric=True)
            # converted['ImageID']=converted['ImageID'].astype(str)
             #converted[['DataTransfer','Ignore','ImageCollectionID','Status','Study']]=converted[['DataTransfer','Ignore','ImageCollectionID','Status','Study']].astype(str)
            
            # list_cpvt=converted.T.to_dict().values()
            # for item in list_cpvt:
            #      table.put_item(Item=item)
    except:
        print(mask)

# change 5: haven't use below
