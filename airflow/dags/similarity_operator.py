from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from neo4j import GraphDatabase

class SimilarityNeo(BaseOperator):

    @apply_defaults
    def __init__(self,*args,**kwargs):
        super(SimilarityNeo, self).__init__(*args, **kwargs)

    def conn(self):
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "lazarus"), encrypted=False)
        session = driver.session()
        return session

    
    def check_graph_exists(self):
        session = self.conn()
        neo_query = "CALL gds.graph.exists('SimilarityGraph') "
        result = session.run(neo_query)
        for record in result:
            exist = record["exists"]
        session.close()

        return exist

    def create_similarity_graph(self):
        
        session = self.conn()
        
        ## OUBLIE PAS DE RETIRER CA 
        neo_query_create = """
        CALL gds.graph.create(
            'SimilarityGraph',
            ['User', 'Media'],
            {
                CONSUMED: {
                type: 'CONSUMED',
                    properties: {
                        strength: {
                            property: 'strength',
                            defaultValue: 1.0
                        }
                    }
                }
            }
        );
        """
        session.run(neo_query_create)
        session.close()

    def determine_score(self):
        session = self.conn()
        neo_query = """
        CALL gds.nodeSimilarity.stream('SimilarityGraph')
        YIELD node1, node2, similarity
        RETURN gds.util.asNode(node1).id AS user1, gds.util.asNode(node2).id AS user2, similarity
        ORDER BY similarity ASCENDING, user1, user2
        """
        session.run(neo_query)
        session.close()
    
    def create_relationship(self):
        session = self.conn()
        neo_query = """
        CALL gds.nodeSimilarity.write('SimilarityGraph', {
            writeRelationshipType: 'SIMILAR',
            writeProperty: 'score'
        })
        YIELD nodesCompared, relationshipsWritten
        """
        session.run(neo_query)
        session.close()

    def delete_similarity_relation(self):
        session = self.conn()
        neo_query = """
        MATCH (u:User)-[r:SIMILAR]->()
        DELETE r
        """
        session.run(neo_query)
        session.close()
    
    def similarity_score(self):
        if self.check_graph_exists:
            self.create_similarity_graph()
        
        self.determine_score()
        self.delete_similarity_relation()
        self.create_relationship()
        

    def execute(self, context):
        self.similarity_score()