from neo4j import GraphDatabase

class recoToUser():

    def __init__(self):
        self.driver = GraphDatabase.driver("neo4j://0.0.0.0:7687", auth=("neo4j", "lazarus"), encrypted=False)
    
    def close(self):
        self.driver.close()
    
    def get_reco_user(self,user_id):
        session = self.driver.session()
        list_media = []
        neo_query = "MATCH (u:User {id:'%s'})-[c:CONSUMED]->(m:Media )<-[:CONSUMED]-(u2:User)-[:CONSUMED]->(m2:Media) WHERE NOT m.id = 'None' AND NOT m2.id = 'None' RETURN m2 LIMIT 10" %user_id
        
        result = session.run(neo_query)

        for record in result:
            media = record['m2'].get('id')
            list_media.append(media)
        session.close()
        return list_media
    
    def relation_user_reco_media(self,user_id):
        list_media = self.get_reco_user(user_id)

        session = self.driver.session()
        for m in list_media:
            neo_query="MATCH (u:User {id:'%s'}) MATCH (m:Media {id:'%s'}) MERGE (u)-[:RECOMMENDATION ]->(m)" %(user_id,m)
            session.run(neo_query)
    
    
