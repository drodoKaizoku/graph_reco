import json
import csv
from dictor import dictor

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase

class RelationPearsonOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(RelationPearsonOperator, self).__init__(*args, **kwargs)


    def execute(self,**context):
        self.bulk_insert_neo4j()
                

    def bulk_insert_neo4j(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()

        neo_query = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_pearson_neo.csv' AS line WITH line MATCH (u1:User {id: line.from}), (u2:User {id:line.to}) MERGE (u1)-[s:SIMILARITY {score: line.similarity}]->(u2) RETURN u1,u2,s"
        session.run(neo_query)
        session.close()