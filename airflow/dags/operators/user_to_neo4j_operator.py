import json
import csv
from dictor import dictor

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase

class UserToNeoOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(UserToNeoOperator, self).__init__(*args, **kwargs)


    def execute(self,**context):
        self.bulk_insert_neo4j()
                

    def bulk_insert_neo4j(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()

        neo_query = "LOAD CSV WITH HEADERS FROM 'file:///bulk_user_gender_neo.csv' AS line WITH line MATCH (u:User {id: line.user_uid }) SET u.birth_year = line.birth_year MERGE (g:Gender {id: line.gender})  MERGE (g)-[:GENDER]->(u) RETURN u,g"
        session.run(neo_query)
        session.close()


