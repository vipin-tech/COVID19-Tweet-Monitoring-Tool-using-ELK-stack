# Load the libraries
import numpy as np
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
import configparser
import h2o
import logging
from pymongo import MongoClient

# Initialize h2o
h2o.init()

# Setup the configuration file
config = configparser.ConfigParser()
config.read('../config/tweet.cfg')

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='../logs/tweet.log',
                    level=logging.DEBUG)

Log = logging.getLogger('../logs/tweet.log')

tweets_file = "../data/tweets.csv"


class MongoDBConnector:
    def __init__(self):
        self.client = MongoClient(config['mongo.client']['MONGO_CLIENT'])
        self.db = self.client.twitter_db
        self.collection = self.db.recent_tweets


class AnalyseTweets(MongoDBConnector):
    def __init__(self):
        MongoDBConnector.__init__(self)
        self.data = self.collection
        self.store_type = config['data.store']['store']
        self.loadData()

    def loadData(self):
        if self.store_type == 'csv':
            self.data = h2o.import_file(tweets_file)
        elif self.store_type == 'mongodb':
            self.data = h2o.H2OFrame(pd.DataFrame(self.data))
        print(self.data)

    def exploreData(self):
        pass

    def visualiseData(self):
        pass

    def buildModels(self):
        pass

    def buildReport(self):
        pass


AnalyseTweets()
