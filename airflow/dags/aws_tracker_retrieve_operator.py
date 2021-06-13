from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from aws_tracker_retrieve_hook import AwsS3Retrieve
from airflow.configuration import conf
from aws_date_tracker_operator import json_to_csv

import base64
import csv
from io import StringIO
import json
from dictor import dictor

from airflow.models import Variable


class RetrieveFileAws(BaseOperator):
    
    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        super(RetrieveFileAws, self).__init__(*args, **kwargs)


    # def json_to_csv(self, tracker_content):
    #     headers = ['user_uid','media_id','media_type','user_device','action']
    #     with open('/usr/local/airflow/logs/import_neo4j/bulk_insert_tracker_neo.csv','w') as outfile:
    #         writer = csv.writer(outfile)
    #         writer.writerow(headers) 
        

    #         for jsonLine in tracker_content:
    #             json_object = json.loads(jsonLine)

    #             user_is_authenticated = str(dictor(json_object,'data.context.user.authenticated'))
    #             media_type_beat = str(dictor(json_object,'type'))
    #             data_action = str(dictor(json_object,'data.context.action'))

    #             ##On prend les utilisateurs connecté
    #             if user_is_authenticated == 'True' and media_type_beat != 'heartbeat' :
    #                 user_uid = str(dictor(json_object,'data.user.uid'))
    #                 user_device = str(dictor(json_object,'data.context.client.device'))

    #                 user_media_id = str(dictor(json_object, 'data.media.id'))
    #                 user_media_type = str(dictor(json_object, 'data.media.type'))
                    
    #                 if data_action == 'None' :
    #                     user_media_id = str(dictor(json_object, 'data.context.screen.media.id'))
    #                     user_media_type = str(dictor(json_object, 'data.context.screen.media.type'))
                    
    #                 data_csv = [user_uid,user_media_id,user_media_type,user_device]
                    
    #                 writer.writerow(data_csv)


    # def video_data(self,tracker_content):
    #     headers = ['user_uid','media_id','media_type','type','action']
    #     with open('/usr/local/airflow/logs/import_neo4j/video_data.csv','w') as outfile:
    #         writer = csv.writer(outfile)
    #         writer.writerow(headers) 

    #         for jsonLine in tracker_content:
    #             json_object = json.loads(jsonLine)

    #             user_is_authenticated = str(dictor(json_object,'data.context.user.authenticated'))
    #             media_type = str(dictor(json_object,'type'))
    #             data_action = str(dictor(json_object,'data.action'))

    #             if user_is_authenticated == 'True' and media_type != 'heartbeat' :

    #                 user_uid = str(dictor(json_object,'data.user.uid'))
    #                 user_media_id = str(dictor(json_object, 'data.media.id'))
    #                 user_media_type = str(dictor(json_object, 'data.media.type'))
                    

    #                 data_csv = [user_uid,user_media_id,user_media_type,data_action]
                    
    #                 writer.writerow(data_csv)



    def execute(self, **kwargs):
        if not self.hook:
            tk = AwsS3Retrieve(prefix=Variable.get("_PREFIX_TRACKER"))
        tracker_content = tk.get_latest_file()


        # self.json_to_csv(tracker_content= tracker_content)
        # self.video_data(tracker_content=tracker_content)
        json_to_csv(tracker_content = tracker_content,file_type='tracker')
                    
