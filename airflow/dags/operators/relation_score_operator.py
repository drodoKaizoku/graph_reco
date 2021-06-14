import json
import csv
from dictor import dictor

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase

class RelationScoreOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(RelationScoreOperator, self).__init__(*args, **kwargs)


    def execute(self,**context):
        self.bulk_insert_neo4j()
                

             
    """ Bulk insert the rating for a media from a user.
    """
    def bulk_insert_neo4j(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()

        neo_query = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_ratio_neo.csv' AS line WITH line MATCH (u:User {id: line.user_uid}), (m:Media {id:line.media_id}) MERGE (u)-[s:SCORE {score: line.score}]->(m) RETURN u,m,s"
        session.run(neo_query)
        session.close()