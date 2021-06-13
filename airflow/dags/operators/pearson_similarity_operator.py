from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase
import pandas as pd
class SimilarityPearsonNeo(BaseOperator):

    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(SimilarityPearsonNeo, self).__init__(*args, **kwargs)

    def conn(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()
        return session
    
    def create_similarity(self):
        session = self.conn()
        
        ## OUBLIE PAS DE RETIRER CA 
        neo_query_create = """
        MATCH (u:User), (m:Media)
        OPTIONAL MATCH (u)-[score:SCORE]->(m)
        WITH {item:id(u), weights: collect(coalesce(toFloat(score.score), gds.util.NaN()))} AS userData
        WITH collect(userData) AS data
        CALL gds.alpha.similarity.pearson.stream({
        data: data,
        topK: 0
        })
        YIELD item1, item2, count1, count2, similarity
        RETURN gds.util.asNode(item1).id AS from, gds.util.asNode(item2).id AS to, similarity
        ORDER BY similarity DESC
        """

        result = session.run(neo_query_create)
        data = result.data()
        df = pd.DataFrame(data)

        df.to_csv("/usr/local/airflow/logs/import_neo4j/bulk_insert_pearson_neo.csv",index=False)
        
    

    def execute(self, context):
        self.create_similarity()