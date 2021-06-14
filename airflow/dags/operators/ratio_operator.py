from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from hook.aws_tracker_retrieve_hook import AwsS3Retrieve

import base64
import csv
from io import StringIO
import json
from dictor import dictor


class RatioOperator(BaseOperator):

    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        
        super(RatioOperator, self).__init__(*args, **kwargs)

    """ Create a csv which will contain for every user the highest ratio on a media that he watched.

        Parameters
        video_tracker: json stored in memory that contain data for every media that a user watched.
    """
    def create_ratio_csv(self, video_tracker):
        headers = ['user_uid','media_id','ratio','pause','score']
        tmp_tab = []
        with open('/usr/local/airflow/logs/import_neo4j/bulk_insert_ratio_neo.csv' ,'w')  as outfile:
            writer = csv.writer(outfile)
            writer.writerow(headers) 

            for jsonLine in video_tracker:
                json_dump = json.dumps(jsonLine)
                json_object = json.loads(json_dump)

                user_id = str(dictor(json_object,'user_uid'))
                media_id = str(dictor(json_object,'media_id'))
                tmp_data = user_id,media_id
                if tmp_data not in tmp_tab:
                    data_csv = self.find_highest_ratio(video_tracker= video_tracker, user_id= user_id,media_id= media_id)
                    writer.writerow(data_csv)
                
                tmp_tab.append(tmp_data)
        
        
    """ Will find the highest ratio for a user and a media. And based on that it will create a rating
    
        Parameters
        video_tracker: json stored in memory that contain data for every media that a user watched.
        user_id: id of the user
        media_id: id of the media that the user watched

        Return the highest ration, with number of time that he paused the media
    """
    def find_highest_ratio(self, video_tracker, user_id, media_id):
        max_ratio = 0
        cpt_pause = 0
        rating = 'None'
        for jsonLine in video_tracker:

            json_dump = json.dumps(jsonLine)
            json_object = json.loads(json_dump)

            user = str(dictor(json_object,'user_uid'))
            media = str(dictor(json_object,'media_id'))
            ratio = str(dictor(json_object,'ratio'))
            pause = str(dictor(json_object,'action'))
            total_time = str(dictor(json_object,'total_time'))
            
            if user_id == user and media == media_id:
                if ratio != 'None':
                    ratio_int = float(ratio)
                    if ratio_int > max_ratio:
                        max_ratio = ratio_int
                    if pause == 'PAUSED':
                        cpt_pause += 1
            if(total_time != 'None'):
                ratio_pause = self.create_ratio_pause(cpt_pause,int(total_time))
                rating = self.create_ratio(max_ratio,ratio_pause)
                
            data_csv = [user_id, media_id, max_ratio,cpt_pause,rating]    

        return data_csv


    """ Will calculate a ratio based on the number of time that he paused a media
        We suppose that he can pause the video every minutes

        Parameters
        pause: number of time that he paused the media
        total_time: length of the media
    """
    def create_ratio_pause(self, pause, total_time):
        if total_time == 0:
            return 0

        return (pause / total_time)
    
    """ Will create a new ratio, based on the ratio minutes watched and number of time that he paused

        Return ratio

    """
    def create_ratio(self, ratio_time, ratio_pause):
        if(ratio_time - ratio_pause) < 0:
            return 0
        value = ((ratio_time - ratio_pause)*10)
        return value

    def execute(self, context):
        video_data = context['ti'].xcom_pull(task_ids='DATE_TRACKER',key='video_tracker')
        self.create_ratio_csv(video_tracker= video_data)
        
