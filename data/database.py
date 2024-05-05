from pymongo import MongoClient
import configparser


class MongoDBHandler:
    def __init__(self, config_file: str):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Read MongoDB credentials
        self.mongodb_cluster = self.config['database']['db_cluster']
        self.mongodb_user = self.config['database']['db_user']
        self.mongodb_pwd = self.config['database']['db_pwd']

        # MongoDB URI
        self.uri = f'''mongodb+srv://{self.mongodb_user}:{self.mongodb_pwd}@{self.mongodb_cluster}.psbuqzs.mongodb.net/?retryWrites=true&w=majority&appName={self.mongodb_cluster}'''

        # Connect to MongoDB cluster
        self.client = MongoClient(self.uri)

    def get_database(self, db_name):
        return self.client[db_name]

    def get_collection(self, db_name, collection_name):
        db = self.get_database(db_name)
        return db[collection_name]

    def close_connection(self):
        self.client.close()
