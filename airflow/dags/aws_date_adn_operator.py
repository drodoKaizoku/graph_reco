from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from aws_tracker_retrieve_hook import AwsS3Retrieve
from aws_adn_retrieve_operator import json_to_csv

import base64
import csv
from io import StringIO
import json
from dictor import dictor


class AwsS3RetrieveDateAdn(BaseOperator):

    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        
        super(AwsS3RetrieveDateAdn, self).__init__(*args, **kwargs)
    

    def execute(self, context):
        if not self.hook:
            self.hook = AwsS3Retrieve(prefix=Variable.get("_PREFIX_ADN"))
        
        date = context['ti'].xcom_pull(task_ids='START_ADN',key='date_adn')
        tracker_content = self.hook.get_latest_file_date(date)

        json_to_csv(tracker_content = tracker_content,file_type='adn_date')
