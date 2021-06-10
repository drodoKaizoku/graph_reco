from airflow.models import BaseOperator
from user_retrieve_hook import UserRetrieveHook
from airflow.utils.decorators import apply_defaults

import csv

class UserRetrieveOperator(BaseOperator):

    @apply_defaults
    def __init__(self,hook ='',*args,**kwargs):
        self.hook = None
        super(UserRetrieveOperator, self).__init__(*args, **kwargs)


    def transform_to_csv(self):
        list_user = self.hook.select_gender()

        with open('/usr/local/airflow/logs/import_neo4j/bulk_user_gender_neo.csv', 'w', newline='') as outfile:
            headers = ['user_uid','gender','birth_year']
            writer = csv.writer(outfile)
            writer.writerow(headers)

            for line in list_user:
                
                data_csv = [line[0],line[1],line[2]]
                writer.writerow(data_csv)

    
    def execute(self, **context):
        if not self.hook:
            self.hook = UserRetrieveHook()
        self.transform_to_csv()

       
        
        