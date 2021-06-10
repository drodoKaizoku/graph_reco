from flask import Flask
import json
from neo4j_service import Neo4jSession
from flask import make_response

app = Flask(__name__)

@app.route("/<user>", methods=['GET'])
def get_reco(user):

    conn = Neo4jSession(uri="neo4j://0.0.0.0:7687", user="neo4j", pwd="lazarus")

    neo_query =  """
    MATCH (u:User {id: '%s'})-[c:CONSUMED]->(m:Media )<-[:CONSUMED]-(u2:User)-[:CONSUMED]->(m2:Media),(u2)-[s:SCORE]->(m2) 
    WHERE NOT m2.id = 'None' AND NOT m.id= 'None' AND NOT ((u)-[:CONSUMED]->(m2)) AND s.score >='0.5' 
    RETURN collect(m2 {.*, Score: s.score}) as RECO
    """ %user
    
    result = conn.query(neo_query)
    
    for record in result:
        response = make_response(json.dumps(record['RECO']))
        response.headers['Content-type'] = "application/json"

    return response




if __name__ == "__main__":
    app.run(port=5000, debug=True)