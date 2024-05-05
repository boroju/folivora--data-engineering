from data.database import MongoDBHandler


class CustomerCollection:
    def __init__(self, config_file):
        self.db_handler = MongoDBHandler(config_file)
        self.customer_db = self.db_handler.get_database('customer_db')
        self.customer_collection = self.db_handler.get_collection('customer_db', 'customer_collection')

    def insert_customer(self, customer_data):
        self.customer_collection.insert_one(customer_data)

    def find(self):
        return self.customer_collection.find()

    def find_customer(self, query):
        return self.customer_collection.find_one(query)

    def update_customer(self, query, update_data):
        self.customer_collection.update_one(query, {'$set': update_data})

    def delete_customer(self, query):
        self.customer_collection.delete_one(query)

    def delete_many(self, query):
        self.customer_collection.delete_many(query)

    def insert_many(self, customers):
        self.customer_collection.insert_many(customers)

    def close_connection(self):
        self.db_handler.close_connection()
