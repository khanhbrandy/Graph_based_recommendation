"""
Creator: khanh.brandy
Created on 2020-09-07

"""

from neo4j import GraphDatabase

class Recommender:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, 
        auth=(user, password),
        encrypted=False
        )

    def close(self):
        self.driver.close()

    @staticmethod
    def testAPI(tx, uid1, uid2):
        score = [uid1, uid2]
        return score


    @staticmethod
    def testNeo4j(tx, uid):
        res = []

        results = tx.run(
            '''match (u1:Agent)
                where u1.agentid= toString($uid)
                return u1.agentid as agentid
            '''
            , uid = uid)
        for result in results:
            r = result['agentid']
            res.append(r)
        return res

    @staticmethod
    def getSimilarity(tx, uid1, uid2):
        res = {}
        results = tx.run(
            '''match (u1:Agent), (u2:Agent)
                where u1.agentid = toString($uid1)
                and u2.agentid = toString($uid2)
                return gds.alpha.similarity.cosine(u1.embedding, u2.embedding) as similarity'''
            , uid1 = uid1, uid2 = uid2)
        for result in results:
            res['uid1'] = uid1
            res['uid2'] = uid2
            res['similarity'] = result['similarity']
        return res

    @staticmethod
    def recommendProducts(tx, uid):
        # Get recommended products based on SIMILAR (Bipartite graph)
        products_1 = {}
        results_1 = tx.run(
        '''match (u1:Agent)-[s:SIMILAR]-(u2:Agent)
            where u1.agentid = toString($uid)
            with u1, s.score as sim_score, u2 order by sim_score DESC 
            MATCH (u2)-[t:TOTAL_AMT]->(p:Product) 
            //WHERE NOT EXISTS( (u1)-[:TOTAL_AMT]->(p) )
            RETURN p.product AS product, SUM( sim_score * t.amount) AS score
            ORDER BY score DESC'''
            , uid = uid)
        for result in results_1:
            item = result['product']
            score = result['score']
            products_1[item] = float(score)
        products_1_dict = {k: v for k, v in sorted(products_1.items(), key=lambda item: item[1], reverse=True)}
        # Get recommended products based on SIMILAR_EBD (Graph embedding)
        products_2 = {}
        results_2 = tx.run(
        '''match (u1:Agent)-[s:SIMILAR_EBD]-(u2:Agent)
            where u1.agentid = toString($uid)
            with u1, s.score as sim_score, u2 order by sim_score DESC 
            MATCH (u2)-[t:TOTAL_AMT]->(p:Product) 
            //WHERE NOT EXISTS( (u1)-[:TOTAL_AMT]->(p) )
            RETURN p.product AS product, SUM( sim_score * t.amount) AS score
            ORDER BY score DESC'''
            , uid = uid)
        for result in results_2:
            item = result['product']
            score = result['score']
            products_2[item] = float(score)
        products_2_dict = {k: v for k, v in sorted(products_2.items(), key=lambda item: item[1], reverse=True)}

        res = set(products_1_dict.keys()).intersection(products_2_dict.keys())
        return res


if __name__ == "__main__":
    # uri = "bolt://13.228.62.137:7687"
    # user = "neo4j"
    # password = "i-00b60e6e3357c9ca7"
    uri = "neo4j://localhost:7687"
    user = "neo4j"
    password = "XXXXXX"
    recommender = Recommender(uri, user, password)
    # Get similarity score
    uid1 , uid2 = 42356225 , 33078074
    with recommender.driver.session() as session:
        res = session.read_transaction(recommender.getSimilarity, uid1 , uid2)
    print(res)
    recommender.close()

    '''
    # Get recommended products
    uid = 43033972
    with recommender.driver.session() as session:
        products = session.read_transaction(recommender.get_recommended_products, uid)
    print(products)
    recommender.close()

    '''