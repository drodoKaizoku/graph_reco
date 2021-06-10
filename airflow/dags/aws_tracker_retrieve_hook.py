import os
import gzip
import shutil
import re

from datetime import date
import gzip
import json

from io import StringIO

from airflow.models import Variable

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook

from dictor import dictor
import datetime

class AwsS3Retrieve(BaseHook):

    def __init__(self, prefix=''):
        self.aws_conn_id = Variable.get("_AWS_CONN_ID")
        self.bucket_name = Variable.get("_BUCKET_NAME")
        self.prefix = prefix
        self.s3_hook = S3Hook(aws_conn_id= self.aws_conn_id)

    
    def retrieve_adn_file(self):
        # year = date.year()
        # month = date.month()
        # prefix_adn = self.prefix + year + "/" + month() + "/"

        # object_keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix= prefix_adn)
        prefix_adn = self.prefix + "2020/11/"
        object_keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix= prefix_adn)

        return object_keys
        

    def retrieve_file(self):
        #prefix date in dir folder tracker
        prefix_date = self.get_date()
        #directory tracker of the last day
        prefix_tracker = self.prefix + prefix_date + "/"
        print(prefix_tracker)
        object_keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix= prefix_tracker)
        return object_keys
    
    def get_date(self):
        today= date.today()
        date_format= today.strftime("%Y%m%d")
        return date_format

    ##RETRIEVE ADN SPECIFIC DATE
    def retrieve_adn_date(self,date):

        date_transformed = datetime.datetime.strptime(str(date),"%Y%m%d").strftime("%Y/%m/")

        print(date_transformed)
        prefix_adn = self.prefix + date_transformed
        object_keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix= prefix_adn)
        
        return object_keys
    
    ##RETRIEVE TRACKER SPECIFIC DATE
    def retrieve_file_date(self, date):
        prefix_tracker = self.prefix + str(date) + '/'
        object_keys = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix= prefix_tracker)
        print(prefix_tracker)
        return object_keys
    
    ##RETURN CONTENT OF THE LATEST FILE OF A SPECIFIC DATE
    def get_latest_file_date(self,date):
        if self.prefix == Variable.get("_PREFIX_TRACKER"):
            file_list = self.retrieve_file_date(date)
            print("ici")
            print(date)
            file_aws = file_list[-1]   
            print(file_aws)
            
        else :
            file_list = self.retrieve_adn_date(date)
            print(date)
            file_aws =  [idx for idx in file_list if str(date) in idx][0]
            
        
        s3_object = self.s3_hook.get_key(file_aws,bucket_name=self.bucket_name)
        
        with gzip.GzipFile(fileobj=s3_object.get()["Body"]) as gzipfile:
            tracker_content = gzipfile.readlines()
            
    
        return tracker_content


    ##RETRIEVE LATEST TRACKER 
    def get_latest_file(self):
        if self.prefix == Variable.get("_PREFIX_TRACKER"):
            file_list = self.retrieve_file()
        else :
            file_list = self.retrieve_adn_file()
        
        print("liste")
        print(file_list)
        latest_file = file_list[-1]
        s3_object = self.s3_hook.get_key(latest_file,bucket_name=self.bucket_name)
        
        with gzip.GzipFile(fileobj=s3_object.get()["Body"]) as gzipfile:
            tracker_content = gzipfile.readlines()
            
    
        return tracker_content

