#adn to neo operator
bulk_adn_insert = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_%s_neo.csv' AS line WITH line MERGE (m:Media {id:line.media_id}) MERGE (t:Type {type:line.media_type}) WITH line, m, t UNWIND split(line.entities_name,';') AS entity UNWIND split(line.topics_name,':') AS topic MERGE(top:Topic {topic: topic}) MERGE (e:Entity {entity: entity}) MERGE (m)-[:TOPIC]->(top) MERGE (m)-[:TYPE]->(t) MERGE (m)-[:ENTITY]->(e)"

#person similarity
create_similarity = """
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


#relation pearson operator
insert_pearson_relation = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_pearson_neo.csv' AS line WITH line MATCH (u1:User {id: line.from}), (u2:User {id:line.to}) MERGE (u1)-[s:SIMILARITY {score: line.similarity}]->(u2) RETURN u1,u2,s"

## SIMILARITY OPERATOR

#check graph exists

check_graph = "CALL gds.graph.exists('SimilarityGraph') "

create_similarity_graph = """
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

determine_score = """
CALL gds.nodeSimilarity.stream('SimilarityGraph')
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).id AS user1, gds.util.asNode(node2).id AS user2, similarity
ORDER BY similarity ASCENDING, user1, user2
"""

create_relationship = """
CALL gds.nodeSimilarity.write('SimilarityGraph', {
    writeRelationshipType: 'SIMILAR',
    writeProperty: 'score'
})
YIELD nodesCompared, relationshipsWritten
"""

delete_similarity_relation = """
MATCH (u:User)-[r:SIMILAR]->()
DELETE r
"""

##tracker to neo4j operator

bulk_insert_tracker = "LOAD CSV WITH HEADERS FROM 'file:///bulk_insert_tracker_neo.csv' AS line WITH line MERGE (u:User {id: line.user_uid}) MERGE (m:Media {id: line.media_id})  MERGE (t:Type {type: line.media_type}) MERGE (d:Device {type: line.user_device}) MERGE (d)-[:DEVICE]->(u) MERGE (u)-[:CONSUMED {strength: 0.5}]->(m) MERGE (m)-[:TYPE]->(t) RETURN u,m,d,t"


## user retrieve hook
sql_query = "SELECT id, gender, birth_year FROM rtbfv2.user WHERE id in {};".format(id_tuple)

## user to neo4j
bulk_insert_gender = "LOAD CSV WITH HEADERS FROM 'file:///bulk_user_gender_neo.csv' AS line WITH line MATCH (u:User {id: line.user_uid }) SET u.birth_year = line.birth_year MERGE (g:Gender {id: line.gender})  MERGE (g)-[:GENDER]->(u) RETURN u,g"
