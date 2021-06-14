from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from hook.aws_tracker_retrieve_hook import AwsS3Retrieve

import base64
import csv
from io import StringIO
import json
from dictor import dictor

import collections

class AwsS3RetrieveDateTracker(BaseOperator):

    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        
        super(AwsS3RetrieveDateTracker, self).__init__(*args, **kwargs)
        
    def execute(self, context):
        if not self.hook:
            self.hook = AwsS3Retrieve(prefix=Variable.get("_PREFIX_TRACKER"))
        
        date = context['ti'].xcom_pull(task_ids='START_TRACKER',key='date_tracker')
        tracker_content = self.hook.get_latest_file_date(date)

        json_to_csv(tracker_content= tracker_content, file_type='tracker')
        video_json = video_data(tracker_content=tracker_content)
        context['ti'].xcom_push(key="video_tracker", value=video_json)



""" Create a CSV file based on a JSON file. 
    Data extracted: user_id, media_type

    Parameters
    tracker_content: content of the JSON file that we load from S3
    file_type: If it's a tracker file or adn.

"""
def json_to_csv(tracker_content,file_type):
    headers = ['user_uid','media_id','media_type','user_device']
    with open('/usr/local/airflow/logs/import_neo4j/bulk_insert_%s_neo.csv' %file_type,'w')  as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers) 
        

        for jsonLine in tracker_content:
            json_object = json.loads(jsonLine)

            user_is_authenticated = str(dictor(json_object,'data.context.user.authenticated'))
            media_type_beat = str(dictor(json_object,'type'))
            data_action = str(dictor(json_object,'data.action'))
            ##On prend les utilisateurs connecté
            if user_is_authenticated == 'True' and media_type_beat != 'heartbeat' :
                user_uid = str(dictor(json_object,'data.user.uid'))
                user_device = str(dictor(json_object,'data.context.client.device'))

                user_media_id = str(dictor(json_object, 'data.media.id'))
                user_media_type = str(dictor(json_object, 'data.media.type'))

                if media_type_beat == 'page' :
                    user_media_id = str(dictor(json_object, 'data.context.screen.media.id'))
                    user_media_type = str(dictor(json_object, 'data.context.screen.media.type'))
              
                data_csv = [user_uid,user_media_id,user_media_type,user_device]
                   
                writer.writerow(data_csv)


""" Create a CSV file based on tracker file. This csv will containt many informations about medias of video type.
    Data in the csv: user_uid, media_id, media_type, action, total_time of the media, current time of the media watched, ratio

    Parameters
    tracker_content: tracker loaded from S3
"""
def video_data(tracker_content):

        json_video = []

        for jsonLine in tracker_content:
            json_object = json.loads(jsonLine)

            user_is_authenticated = str(dictor(json_object,'data.context.user.authenticated'))
            media_type = str(dictor(json_object,'data.media.type'))
            tracker_type = str(dictor(json_object,'type'))
            data_action = str(dictor(json_object,'data.action'))
            
            if user_is_authenticated == 'True' and tracker_type != 'heartbeat' and media_type == 'video' :
                user_uid = str(dictor(json_object,'data.user.uid'))
                user_media_id = str(dictor(json_object, 'data.media.id'))
                user_media_type = str(dictor(json_object, 'data.media.type'))
                    
                total_time = str(dictor(json_object, 'data.context.media.total_time'))
                current_time = str(dictor(json_object, 'data.context.media.current_time'))
                if total_time == '0' or (total_time == 'None' or current_time == 'None') :
                    ratio = 'None'
                else :
                    ratio = float(current_time)/float(total_time)

                data_set = {"user_uid": user_uid, "media_id": user_media_id, "media_type": user_media_type,"action": data_action,"total_time": total_time,"current_time":current_time,"ratio":ratio}    

                json_dump = json.dumps(data_set)
                json_obj = json.loads(json_dump)
                json_video.append(json_obj)

        return json_video
                

        

