from neo4j import GraphDatabase


class Neo4jSession():

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        

    def query(self, query, db=None):
        try: 
            driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            session = driver.session(database=db) if db is not None else driver.session() 
            response = list(session.run(query))
            driver.close()

        except Exception as e:
            print(e)
            return None
        if len(response) == 0:
            return None
        return response