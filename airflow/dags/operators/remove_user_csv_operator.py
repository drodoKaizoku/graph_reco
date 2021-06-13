import csv
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RemoveUserCsv(BaseOperator):

    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(RemoveUserCsv, self).__init__(*args, **kwargs)
    

    def delete_file_csv(self):
        directory = "/usr/local/airflow/logs/import_neo4j/"

        files_in_directory = os.listdir(directory)
        filtered_files = [file for file in files_in_directory if file.endswith(".csv")]
        for file in filtered_files:
            path_to_file = os.path.join(directory, file)
            os.remove(path_to_file)

    def execute(self,**context):
        self.delete_file_csv()