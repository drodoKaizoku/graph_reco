import json
import csv
from dictor import dictor

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase
import neo_request

class adnToNeo4j(BaseOperator):

    @apply_defaults
    def __init__(self,adn='',*args,**kwargs):
        self.adn = adn
        super(adnToNeo4j, self).__init__(*args, **kwargs)


    def execute(self,**context):
        self.bulk_insert_neo4j()
                
    """ Bulk insert a csv which contain adn of content into neo4j database 
    """
    def bulk_insert_neo4j(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()

        neo_query = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_%s_neo.csv' AS line WITH line MERGE (m:Media {id:line.media_id}) MERGE (t:Type {type:line.media_type}) WITH line, m, t UNWIND split(line.entities_name,';') AS entity UNWIND split(line.topics_name,':') AS topic MERGE(top:Topic {topic: topic}) MERGE (e:Entity {entity: entity}) MERGE (m)-[:TOPIC]->(top) MERGE (m)-[:TYPE]->(t) MERGE (m)-[:ENTITY]->(e)" %self.adn
        session.run(neo_query)
        session.close()