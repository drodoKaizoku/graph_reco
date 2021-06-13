import json
import csv
from dictor import dictor

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase

class trackerFormatData(BaseOperator):
    
    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(trackerFormatData, self).__init__(*args, **kwargs)


    def execute(self,**context):
        self.bulk_insert_neo4j()
                

    def bulk_insert_neo4j(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()

        neo_query = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_tracker_neo.csv' AS line WITH line MERGE (u:User {id: line.user_uid}) MERGE (m:Media {id: line.media_id})  MERGE (t:Type {type: line.media_type}) MERGE (d:Device {type: line.user_device}) MERGE (d)-[:DEVICE]->(u) MERGE (u)-[:CONSUMED {strength: 0.5}]->(m) MERGE (m)-[:TYPE]->(t) RETURN u,m,d,t"
        session.run(neo_query)
        session.close()