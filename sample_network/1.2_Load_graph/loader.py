"""
Creator: khanh.brandy
Created on 2020-09-07

"""

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

class Loader:

    def __init__(self):
        self.uri = "neo4j://localhost:7687"
        self.user = "neo4j"
        self.password = "XXXXXXX"
        self.driver = GraphDatabase.driver(self.uri, 
        auth=(self.user, self.password),
        encrypted=False
        )

    def close(self):
        self.driver.close()

    @staticmethod
    def runQuery(tx, query):
        tx.run(query)
        print('Done Query execution!')

    @staticmethod
    def loadUsers(tx, file_path):
        query = '''
        //=================================
        //== LOAD USERS
        //=================================
        CALL apoc.periodic.iterate(
                "CALL apoc.load.csv('$file_path', {skip:0, limit:90000, header:true,
                mapping:{
                    agentid: {type:'string'},
                    in_degree: {type:'float'},
                    out_degree: {type:'float'},
                    page_rank: {type:'float'},
                    community_id: {type:'string'},
                    category: {type:'string'},
                    status: {type:'string'},
                    user_type: {type:'string'}     
                }
                })
                yield map as row", 
                "MERGE (a:Agent {agentid: row.agentid})
                ON CREATE SET  a.in_degree = row.in_degree,
                a.out_degree = row.out_degree,
                a.page_rank = row.page_rank,
                a.community_id = row.community_id,
                a.category = row.category,
                a.status = row.status,
                a.user_type = row.user_type"
                , {batchSize:100, iterateList:true, parallel:true})'''.replace('$file_path', file_path)
        tx.run(query)
        print('Loading Users list: {}'.format(file_path))

    @staticmethod
    def loadTrans(tx, file_path):
        query = '''
        //=================================
        //== LOAD TRANSACTIONS
        //=================================
                    LOAD CSV WITH HEADERS FROM 'file:///$file_path' AS row
                    WITH row.u1_agentid AS u1_agentid,
                    row.u1_reference as u1_reference,
                    row.tranid AS tranid, 
                    toFloat(row.amount) AS amount, 
                    row.tran_type as tran_type,
                    row.u2_agentid as u2_agentid,
                    row.u2_reference as u2_reference
                    MATCH (u1:Agent {agentid: u1_agentid})
                    MATCH (u2:Agent {agentid: u2_agentid})
                    MERGE (u1)-[rel:TRANSACTION {u1_reference: u1_reference, tranid : tranid , amount: amount, tran_type: tran_type, u2_reference:u2_reference}]->(u2)
                    RETURN count(rel)'''.replace('file:///$file_path', file_path)
        tx.run(query)
        print('Loading Trans list: {}'.format(file_path))
        
    @staticmethod
    def createGraph(tx):
        query = '''
        //=================================
        //CREATE (temporary) GRAPH
        //=================================
        CALL gds.graph.create('recommendv4', 'Agent', 'TRANSACTION', {
                nodeProperties: ['in_degree', 'out_degree','page_rank']
            })
        YIELD graphName, nodeCount, relationshipCount'''
        tx.run(query)
        print('Creating temp graph...')
    
    @staticmethod
    def runEmbedding(tx):
        query = '''
        //=================================
        //NODE EMBEDDING
        //=================================
        CALL gds.alpha.graphSage.write(
        'recommendv4',
        {
            writeProperty: 'embedding',
            nodePropertyNames: ['in_degree', 'out_degree', 'page_rank'],
            aggregator: 'mean',
            activationFunction: 'sigmoid',
            embeddingSize: 15,
            sampleSizes: [25, 1000],
            batchSize: 1000,
            epochs: 100,
            degreeAsProperty: true
        }
        ) YIELD startLoss, epochLosses'''
        tx.run(query)
        print('Start embedding nodes...')

    @staticmethod
    def loadProducts(tx, file_path):
        query = '''
        //=================================
        //== LOAD PRODUCTS
        //=================================

        LOAD CSV WITH HEADERS FROM 'file:///$file_path' AS row
        WITH row.product AS product
        MERGE (p:Product {product: product})
        SET p.product = product
        RETURN count(p)'''.replace('file:///$file_path', file_path)
        try: 
            tx.run(query)
            print('Loading Product list: {}'.format(file_path))
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
        

    @staticmethod
    def createTotalAmt(tx):
        query = '''
        //=================================
        //== CREATE TOTAL_AMT
        //=================================

        MATCH (u:Agent),(p:Product)
        where EXISTS( (u)-[:TRANSACTION {tran_type: p.product}]->() )
        CREATE (u)-[r:TOTAL_AMT]->(p)
        RETURN type(r)'''
        try: 
            tx.run(query)
            print('Creating Total amount (relationship)...')
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def updateTotalAmt(tx):
        query = '''
        //=================================
        //UPDATE TOTAL_AMT
        //=================================
        MATCH (u:Agent)-[t:TRANSACTION]-()
        with u.agentid as id, t.tran_type as prod, sum(t.amount) as TPV
        match (u:Agent)-[r:TOTAL_AMT]->(p:Product) where u.agentid = id and p.product = prod
        set r.amount = TPV
        return u.agentid , r.amount, p.product'''
        try: 
            tx.run(query)
            print('Updating Total amount (relationship)...')
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def calculateSimilarity(tx):
        query = '''
        //=================================
        //CALCUALTE PEARSON SIMILARITY USING NEO4J PROCEDURE
        //=================================

        MATCH (u:Agent), (p:Product)
        OPTIONAL MATCH (u)-[r:TOTAL_AMT]->(p) where p.product <> "adjustment"
        WITH {item:id(u), weights: collect(coalesce(r.amount, 0))} AS userData
        WITH collect(userData) AS data
        CALL gds.alpha.similarity.pearson.write({
        data: data,
        topK: 1,
        similarityCutoff: -1)
        YIELD nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
        RETURN nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p95'''
        try: 
            tx.run(query)
            print('Calculating and writing Similarity (relationship)...')
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    @staticmethod
    def createSimilarEBD(tx):
        query = '''
        //=================================
        //CREATE SIMILAR_EBD (use embedding as Vector)
        //=================================

        MATCH (u1:Agent),(u2:Agent)
        where EXISTS( (u1)-[:SIMILAR]->(u2) )
        CREATE (u1)-[r:SIMILAR_EBD]->(u2)
        RETURN type(r)'''
        try: 
            tx.run(query)
            print('Creating new Simillar_EBD (relationship)...')
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    
    @staticmethod
    def updateSimilarityEBD(tx):
        query = '''
        //=================================
        // UPDATE SIMILAR_EBD 
        //=================================
        //Pearson similarity
        match (u1:Agent)-[r:SIMILAR_EBD]-(u2:Agent)
        with u1.agentid as u1_agentid, r, 
        [u1.embedding[0], u1.embedding[1], u1.embedding[2], u1.embedding[3], u1.embedding[4], u1.embedding[5], 
        u1.embedding[6], u1.embedding[7], u1.embedding[8], u1.embedding[9], u1.embedding[10],
        u1.embedding[11], u1.embedding[12], u1.embedding[13], u1.embedding[14]] as u1_embedding, 
        u2.agentid as u2_agentid, 
        [u2.embedding[0], u2.embedding[1], u2.embedding[2], u2.embedding[3], u2.embedding[4], u2.embedding[5], 
        u2.embedding[6], u2.embedding[7], u2.embedding[8], u2.embedding[9], u2.embedding[10],
        u2.embedding[11], u2.embedding[12], u2.embedding[13], u2.embedding[14]] as u2_embedding
        with u1_agentid, u2_agentid, r, gds.alpha.similarity.pearson(u1_embedding, u2_embedding) as similarity 
        set r.score = similarity
        return u1_agentid , r.score, u2_agentid'''
        try: 
            tx.run(query)
            print('Updating Simillar_EBD (relationship)...')
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
        
def run_all(load_user = True, load_trans = True, create_graph = True, node_embedding = True, 
            load_product = True, create_totalamt = True, update_totalamt = True, 
            calculate_similarity = True, create_similar_ebd = True, update_similar_ebd = True):
    #0 Start driver
    # print('*'*20)
    loader = Loader()
    user_path = 'agent.csv'
    trans_paths = ['file:///transaction_1.csv', 'file:///transaction_2.csv']
    product_path = 'file:///product.csv'
    #1 Load Users 
    if load_user:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.loadUsers, user_path)
            print('Done loading {}!'.format(user_path))
    #2 Load Transaction 
    if load_trans:
        for trans_path in trans_paths:
            with loader.driver.session() as session:
                print('*'*20)
                session.write_transaction(loader.loadTrans, trans_path)
                print('Done loading {}!'.format(trans_path))
    #3 Create temp graph
    if create_graph:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.createGraph)
            print('Done creating temp graph!')
    #4 Node embedding 
    if node_embedding:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.runEmbedding)
            print('Done embedding nodes!')
    #5 Load Products 
    if load_product:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.loadProducts, product_path)
            print('Done loading {}!'.format(product_path))

    #6 Create Total amount
    if create_totalamt:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.createTotalAmt)
            print('Done creating Total amount (relationship)!')

    #7 Update Total amount
    if update_totalamt:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.updateTotalAmt)
            print('Done updating Total amount (relationship)!')

    #8 Calculate and Write Similarity
    if calculate_similarity:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.calculateSimilarity)
            print('Done calculating and writing Similarity (relationship)!')

    #9 Creat new Simillar_EBD relationships based on Similar relationships created above 
    if create_similar_ebd:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.createSimilarEBD)
            print('Done creating new Simillar_EBD (relationship)!')
    
    #10 Updating Simillar_EBD relationships based on embedded vectors  
    if create_similar_ebd:
        with loader.driver.session() as session:
            print('*'*20)
            session.write_transaction(loader.createSimilarEBD)
            print('Done updating Simillar_EBD (relationship)!')
            

    #-1 Close driver
    loader.close()


if __name__ == "__main__":
    run_all(
        load_user = False, 
        load_trans = False, 
        create_graph = False, 
        node_embedding = False, 
        load_product = False,
        create_totalamt = False,
        update_totalamt = False,
        calculate_similarity = False,
        create_similar_ebd = True,
        update_similar_ebd = False
        )
