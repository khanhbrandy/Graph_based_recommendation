"""
Creator: khanh.brandy
Created on 2020-06-30

"""
import sys
from flask import Flask
from flask import request, abort
from flask import jsonify

import os.path
import datetime
import pickle
import pandas as pd 
from modules.recommendation import Recommender


class RecommendationAPI():
    '''
    Recommendation API:

    * Remote Neo4j server:
    uri = "bolt://13.228.62.137:7687"
    user = "neo4j"
    password = "XXXXXXX"

    * Local Neo4j server:
    uri = "neo4j://localhost:7687"
    user = "neo4j"
    password = "XXXXXXXX"

    '''
    def __init__(self):
        self.app = Flask(__name__)
        self.uri = "neo4j://localhost:7687"
        self.user = "neo4j"
        self.password = "123456"
        self.recommender = Recommender(self.uri, self.user, self.password)
        # self.app.config["DEBUG"] = True
        self.app.config['development'] = True
        self.app.config['JSON_AS_ASCII'] = False
        self.app.config['JSON_SORT_KEYS'] = False
        self.app.debug = True
        @self.app.route('/')
        def home():
            return '''<h1>Hello from the other side!</h1>
                    <p>A prototype API for Momo recommendation.</p>'''
        @self.app.route('/v4/similarity', methods=['GET'])
        def getSimilarity():
            '''
            Sample request : http://localhost:8080/v4/similarity?uid1=43033972&uid2=33078074
            '''
            uid1 = int(request.args['uid1'])
            uid2 = int(request.args['uid2'])
            if (uid1 is None):
                return 'ID1 invalid!'
            elif (uid2 is None):
                return 'ID2 invalid!'
            else:
                with self.recommender.driver.session() as session:
                    res = session.read_transaction(self.recommender.getSimilarity, uid1 , uid2)
                # self.recommender.close()
                return jsonify(res)
        @self.app.route('/v4/product', methods=['GET'])
        def recommendProducts():
            '''
            Sample request : http://localhost:8080/v4/product?uid=42356225
            '''
            uid = int(request.args['uid'])
            if (uid is None):
                return 'ID invalid!'
            else:
                with self.recommender.driver.session() as session:
                    res = session.read_transaction(self.recommender.recommendProducts, uid)
                # self.recommender.close()
                return jsonify(res)
        
    
    def run(self, port):
        # self.app.run(host='0.0.0.0',port=port)
        self.app.run(host='localhost', port=port)

if __name__=='__main__':
    args = sys.argv
    if len(args) > 1:
        port = int(args[1])
    else:
        port = 8080
    service = RecommendationAPI()
    service.run(port)

    '''
    Sample request : http://localhost:8080/v4?id=29223069
    '''