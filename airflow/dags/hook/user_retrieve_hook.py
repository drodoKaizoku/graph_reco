from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.connection import Connection
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

import csv
import pandas

class UserRetrieveHook(BaseHook):

    def __init__(self):
        self.postgres_hook = PostgresHook(postgres_conn_id = Variable.get("_POSTGRES_CONN_ID"))
        self.connection = self.postgres_hook.get_conn()
        
    def select_gender(self):
        user_uid = self.retrieve_id_user_csv()
        id_tuple = tuple(user_uid)
        sql_query = "SELECT id, gender, birth_year FROM rtbfv2.user WHERE id in {};".format(id_tuple)

        cursor = self.connection.cursor()
        cursor.execute(sql_query)
        sources = cursor.fetchall()

        return sources

    def retrieve_id_user_csv(self):
        
        colnames = ['user_uid','media_id','media_type','user_device']
        data = pandas.read_csv('/usr/local/airflow/logs/import_neo4j/bulk_insert_tracker_neo.csv',names=colnames)

        user_uid = data.user_uid.tolist()

        return user_uid[1:]