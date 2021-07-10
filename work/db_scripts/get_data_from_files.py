#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 11 13:59:26 2018

@author: humbertotalavera
"""

import paramiko
from paramiko import SSHClient
#import glob 
import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
import pandas as pd
import numpy as np
#import stat
#import re
import os
import sqlalchemy
import subprocess as sb
from sqlalchemy import create_engine
import luigi

now = datetime.datetime.now()
today=now.strftime("%Y-%m-%d")


def softlayer_connect():
    # Auth
    host = "10.177.153.194"
    port = 22
    username=input('Enter your softlayer username: ')
    password=input('Enter your softlayer password: ')
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return host,username,password,sftp,transport

try: 
    transport.is_active()
except:
    host,username,password,softlayer,transport=softlayer_connect() 
else:
    print('Already Connected')
# Go!

#Connect to MYSQL database####
Base = automap_base()
engine = create_engine('mysql+mysqlconnector://test:test_pa$$word@localhost/SMD_Images', echo=False)
staging=create_engine('mysql+mysqlconnector://test:test_pa$$word@localhost/SMD_Staging', echo=False)
Base.prepare(engine, reflect=True)
session = Session(engine)

Fact=Base.classes.fact_images

max_index=session.query(func.max(Fact.Indexvalue)).scalar()
#max_index=52193

def check_endPath(fp):
    if fp.endswith('/'):
        return fp
    else:
        return fp + '/'

    
class ScrapeFileStructure(luigi.Task):
    def output(self):
        return luigi.LocalTarget('/Users/humbertotalavera/AnacondaProjects/Burn.csv')

    def run(self):
        df=pd.DataFrame()
        fpath='//disk4/WakeForest_poc_GMP1_11_27_2018/SpectralView/'
        x=softlayer.listdir(fpath)  
        for mode in ['Burn','Chronic_Wounds','Blood_Flow_Study']:
            for p in x:
                if p.startswith('Patient'):
                    #print(p)
                    try:
                        b=softlayer.listdir(fpath+str(p)+'/'+mode+'/')
                        pass
                    except:
                        continue
                    for s in b:
                        if s.startswith('Study'):
                            #print(s)
                            if(int(s.split('Study')[1])>0):
                                d=softlayer.listdir(fpath+str(p)+'/'+mode+'/'+str(s)+'/')
                                for a in d: 
                                    #print(a)
                                    t=softlayer.listdir(fpath+str(p)+'/'+mode+'/'+str(s)+'/'+str(a)+'/')
                                    for n in t: 
                                        #print(n)
                                        i=softlayer.listdir(fpath+str(p)+'/'+mode+'/'+str(s)+'/'+str(a)+'/'+str(n)+'/')
                                        for m in i:
                                            #print(m)
                                            collection=softlayer.listdir(fpath+str(p)+'/'+mode+'/'+str(s)+'/'+str(a)+'/'+str(n)+'/'+str(m))
                                            for images in collection:
                                                if images.endswith(('.tiff','.tif','png')):
                                                    path=fpath+str(p)+'/'+mode+'/'+str(s)+'/'+str(a)+'/'+str(n)+'/'+str(m)+'/'+images
                                                    if datetime.datetime.fromtimestamp(softlayer.stat(path).st_mtime)> datetime.datetime(2018,5,1,1,22,30):
                                                        ImageDate=softlayer.stat(path).st_mtime
                                                        df = df.append({'Patient': p, 'Study': s, 'Anatomical': a, 'Series': n, 'Anatomical': a, 'ImageCollection': m, 
                                                                        'ImageName': images, 'Module':mode,'FileName':path,'ImageDate':ImageDate}, ignore_index=True)    
        mask_df=pd.DataFrame()
        fpath='/disk4/all_mask_images_png/'
        x=softlayer.listdir(fpath)  
        for images in x:
            if images.startswith(('Mask','PseudoColor')):
                path=fpath+images
                if datetime.datetime.fromtimestamp(softlayer.stat(path).st_mtime)> datetime.datetime(2018,11,10):
                    splits=images.split('_')
                    #when patiend ID is normal 
                    #p='Patient' +str(int(splits[1])+4)
                    #patient id with dash
                    if '-' in splits[1]:
                        p=int(splits[1].split('-')[1])
                    else:
                        p=int(splits[1])
                    if p==15:
                        pid='Patient'+str(18)
                    elif p==14:
                        pid='Patient'+str(19)
                    else:
                        pid='Patient'+str(p+4)        
                    m=splits[2]
                    s=splits[3]
                    a=splits[4]
                    n=str(splits[5])
                    ic=splits[6]+'_'+splits[7]    
                    ImageDate=softlayer.stat(path).st_mtime
                    mask_df=mask_df.append({'Patient': pid, 'Study': s, 'Anatomical': a, 'Series': n, 'Anatomical': a, 'ImageCollection': ic, 
                                            'ImageName': images, 'Module':'Burn','FileName':path,'ImageDate':ImageDate}, ignore_index=True)
        frames=[df,mask_df]
        final_df=pd.concat(frames)                        
        f = self.output().open('w')
        final_df.to_csv(f)
        f.close()

#
        
#USE THIS FOR REAL DATA

        
class UpdateDataFrame(luigi.Task):
    def requires(self):
        return ScrapeFileStructure()
    def output(self):
        return luigi.LocalTarget('/Users/humbertotalavera/AnacondaProjects/ImageData_v4.csv')

    def run(self):
        df=pd.read_csv(self.input().open('r'), sep=',',dtype=str)
        df['Frequency']=df['ImageName'].str.split('_').str[2]
        
        df['PID']=df.Patient.str.extract('(\d+)')
        df['SID']=df.Study.str.extract('(\d+)')
        df['ImageDate_Formatted']=pd.to_datetime(df['ImageDate'], unit='s')
        
        df=df.reset_index()
        df['IndexValue']=df.index.copy()+max_index+1
        df['DirFloat']=np.ceil((df['IndexValue'])/1000)
        df['DirNumber']=df['DirFloat'].astype(int)
        ext=df['ImageName'].str.split('.').str[-1]
        df['ParentPath']='/disk2/humberto/POC_Images2/'
        df['newPath']=df['ParentPath'].astype(str)+df['DirNumber'].astype(str)+'/'+df['IndexValue'].astype(str)+'_'+df['PID'].astype(str)+'_'+df['SID'].astype(str)+'.'+ext
        df['Series']=df['Series'].astype(str)
        #df['FileName']=df['FileName'].str.replace('/disk4/WakeForest_poc_GMP1_01_23_2018/','/disk4/WakeForest_poc_GMP1_12_13_2017/SpectralView/')
        df['ImgCollName']='D:\\SpectralView\\'+df['Patient'].astype(str)+'\\'+df['Module']+'\\'+df['Study'].astype(str)+'\\'+df['Anatomical'].astype(str)+'\\'+df['Series'].astype(str)+'\\'+df['ImageCollection'].astype(str)+'\\'
        f = self.output().open('w')
        df.to_csv(f)
        f.close()
        

class MoveDataFrame(luigi.Task):
    def requires(self):
        return UpdateDataFrame()

    def output(self):
        return print('Dataframe has been transferred to server1')

    def run(self):
        localfile='/Users/humbertotalavera/AnacondaProjects/ImageData_v4.csv'
        remotefile='/disk2/humberto/ImageData_v4.csv'
        softlayer.put(localfile,remotefile)

class UpdateDataBase(luigi.Task):
    def requires(self):
        return UpdateDataFrame()

    def output(self):
        return print('Complete')

    def run(self):
        df=pd.read_csv(self.input().open('r'))
        df.to_sql(name='ImageData_v4', con=staging, if_exists = 'replace', index=False, index_label='index', dtype={'SeriesInstance':sqlalchemy.types.VARCHAR(length=255)},chunksize=1000)

class BackupDB(luigi.Task):
    def requires(self):
        return UpdateDataFrame()
    
    def output(self):
        return luigi.LocalTarget('/Users/humbertotalavera/AnacondaProjects/'+today+'_SMD_Images.sql')
    
    def run(self):
        x=sb.Popen("mysqldump --user=test --password='test_pa$$word' --opt SMD_Images > /Users/humbertotalavera/AnacondaProjects/"+today+"_SMD_Images.sql", shell=True)
        x.communicate()
        
class UpdateLegacyFiles(luigi.Task):
    def requires(self):
        return BackupDB()
    
    def output(self):
        return print('Complete')
    
    def run(self):
        filename='/Users/humbertotalavera/Documents/SQLCode/SoftwareData.sql'
        engine.execute('USE SMD_staging')
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        sqlCommands = sqlFile.split(';')
    
        for command in sqlCommands:
            try:
                if command.strip() != '':
                    engine.execute(command)
            except IOError as msg:
                print("Command skipped: ", msg)

class FixIssues(luigi.Task):
    def requires(self):
        return BackupDB()
    
    def output(self):
        return print('Complete')
    
    def run(self):
        filename='/Users/humbertotalavera/Documents/SQLCode/fixissues.sql'
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        sqlCommands = sqlFile.split(';')
    
        for command in sqlCommands:
            try:
                if command.strip() != '':
                    engine.execute(command)
            except IOError as msg:
                print("Command skipped: ", msg)
                
                
class UpdateProductionSchema(luigi.Task):
    def requires(self):
        return BackupDB()
    
    def output(self):
        return print('Complete')
    
    def run(self):
        filename='/Users/humbertotalavera/Documents/SQLCode/factdimSchemaUpdateNew.sql'
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        sqlCommands = sqlFile.split(';')
    
        for command in sqlCommands:
            try:
                if command.strip() != '':
                    engine.execute(command)
            except IOError as msg:
                print("Command skipped: ", msg)
                
class UpdateSMD_Images(luigi.Task):
    def requires(self):
        return BackupDB()
    
    def output(self):
        return print('Complete')
    
    def run(self):
        filename='/Users/humbertotalavera/Documents/SQLCode/facttablecreation_STAGING.sql'
        engine.execute('USE SMD_staging')
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        sqlCommands = sqlFile.split(';')
    
        for command in sqlCommands:
            try:
                if command.strip() != '':
                    engine.execute(command)
            except IOError as msg:
                print("Command skipped: ", msg)
                


class MoveImages(luigi.Task):
    def requires(self):
        return UpdateDataFrame()
    
    def output(self):
        return print('Complete')
    
    def run(self):
        client = SSHClient()
        client.load_system_host_keys()
        client.connect(host, username=username, password=password)
        stdin, stdout, stderr = client.exec_command('nohup python3 -u /disk2/humberto/POC_Python_Scripts/MoveImages.py')
        print("stderr: ", stderr.readlines())
        print("pwd: ", stdout.readlines())
        

        #print("stderr: ", stderr.readlines())
        #print("pwd: ", stdout.readlines())

if __name__ == '__main__':
    luigi.build([ScrapeFileStructure(),UpdateDataFrame(),UpdateDataBase(),MoveDataFrame(),BackupDB(),UpdateLegacyFiles(),UpdateProductionSchema(),FixIssues(),UpdateSMD_Images()])




#class CountLetters(luigi.Task):
#
#    def requires(self):
#        return GenerateWords()
#
#    def output(self):
#        return luigi.LocalTarget('letter_counts.txt')
#
#    def run(self):
#
#        # read in file as list
#        with self.input().open('r') as infile:
#            words = infile.read().splitlines()
#
#        # write each word to output file with its corresponding letter count
#        with self.output().open('w') as outfile:
#            for word in words:
#                outfile.write(
#                        '{word} | {letter_count}\n'.format(
#                            word=word,
#                            letter_count=len(word)
#                            )
#                        )
