from pymongo import MongoClient
from config_reader import ConfigReader


class MongoDBHandler:
    def __init__(self):
        self.config_reader = ConfigReader()

        # Read MongoDB credentials from ConfigReader
        db_config = self.config_reader.get_database_config()
        self.mongodb_cluster = db_config['cluster']
        self.mongodb_user = db_config['user']
        self.mongodb_pwd = db_config['pwd']

        # MongoDB URI
        self.uri = f'''mongodb+srv://{self.mongodb_user}:{self.mongodb_pwd}@{self.mongodb_cluster}.psbuqzs.mongodb.net/?retryWrites=true&w=majority&appName={self.mongodb_cluster}'''

        # Connect to MongoDB cluster
        self.client = MongoClient(self.uri)

    def get_database(self, db_name):
        return self.client[db_name]  # Accessing the database directly here

    def get_collection(self, db_name, collection_name):
        db = self.get_database(db_name)  # Get the database
        return db[collection_name]  # Accessing the collection directly here

    def close_connection(self):
        self.client.close()


def main():
    # Create an instance of MongoDBHandler
    mongo_handler = MongoDBHandler()

    # Access a database and collection
    db_name = "customer_db"
    collection_name = "customer_collection"
    collection = mongo_handler.get_collection(db_name, collection_name)

    # Example: Count documents in the collection
    document_count = collection.count_documents({})
    print(f"Total documents in '{collection_name}': {document_count}")

    # Close connection
    mongo_handler.close_connection()


if __name__ == "__main__":
    main()
