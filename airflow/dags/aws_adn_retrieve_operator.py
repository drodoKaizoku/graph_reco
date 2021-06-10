from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from aws_tracker_retrieve_hook import AwsS3Retrieve

import base64
import csv
from io import StringIO
import json
from dictor import dictor

from airflow.models import Variable

class AwsS3RetrieveAdn(BaseOperator):

    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        super(AwsS3RetrieveAdn, self).__init__(*args, **kwargs)


    def execute(self, **context):
        if not self.hook:
            self.hook = AwsS3Retrieve(prefix=Variable.get("_PREFIX_ADN"))
        tracker_content = self.hook.get_latest_file()
        json_to_csv(tracker_content = tracker_content,file_type='adn')


def json_to_csv(tracker_content,file_type):
        headers = ['media_id','media_type','entities_name','topics_name']
        with open('/usr/local/airflow/logs/import_neo4j/bulk_insert_%s_neo.csv' %file_type,'w')  as outfile:
            writer = csv.writer(outfile)
            writer.writerow(headers) 
        
            for jsonLine in tracker_content:
                json_object = json.loads(jsonLine)

                media_id = str(dictor(json_object, 'media_id'))
                media_type = str(dictor(json_object, 'media_type'))
                entities = dictor(json_object, 'metadata.entities')
                topics = dictor(json_object, 'metadata.topics')

                entities_name = ""
                topics_name = ""
                for entity in entities:
                    entities_name += ";"
                    entities_name += str(entity['name'])
                
                for topic in topics:
                    topics_name += ";"
                    topics_name += str(topic['name'])

                data_csv = [media_id,media_type,entities_name[1:],topics_name[1:]]
                writer.writerow(data_csv)
    
