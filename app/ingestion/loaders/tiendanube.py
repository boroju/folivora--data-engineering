from app.ingestion.loaders.utils import BearerAuthApi
from app.ingestion.loaders.utils import logging
from app.ingestion.loaders.utils import Dict, List
from datetime import datetime
from app import ROOT_DIR
from app.db.database import DuckDBDatabase
from app.ingestion.loaders.parsers.parse_orders import parse_orders
from app.ingestion.loaders.parsers.parse_customers import parse_customers
from app.ingestion.loaders.parsers.parse_abandoned_checkouts import parse_abandoned_checkouts
from app.ingestion.loaders.parsers.parse_categories import parse_categories
from app.ingestion.loaders.parsers.parse_products import parse_products
import pandas as pd
import os


class TiendanubeLoader:
    name = "tiendanube_loader"

    def __init__(self,
                 api_key: str = None,
                 api_host: str = None,
                 load_type: str = None):
        """ Init function to build TiendanubeLoader
        Args:
            api_key: API key for authentication
            api_host: API host URL
            load_type: with the possible following options to load data --> all_customers or all_orders
        Returns:
            Nothing
        """

        # Dates to a "%Y%m%d"
        self.processed_date = str(datetime.now().date().strftime("%Y%m%d"))

        # Api Authorization
        self.api = BearerAuthApi(
            api_key=api_key,
            host=api_host
        )
        self.load_type = load_type
        self.db = DuckDBDatabase(
            db_path=os.path.join(ROOT_DIR, "db/files/folivora_tiendanube", "folivora_tiendanube.duckdb"))

    def load(self):

        if self.load_type == "all_customers":
            logging.info("Executing TiendanubeLoader call for getting All Customers...")

            logging.info(f"Executing api calls...")
            customers = self.get_all_customers()
            logging.info(f"Creating customers_df.")
            customers_df = pd.DataFrame(customers)

            logging.info("Dataframe customers_df Columns:")
            logging.info(customers_df.columns)
            logging.info("Dataframe customers_df Head(5):")
            logging.info(customers_df.head(5))

            logging.info(f"Connect to the database...")
            con = self.db.connect()

            logging.info(f"Saving customers data into database...")

            logging.info(f"Creating schema [raw] if not exists...")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS raw;")

            logging.info(f"Creating table raw.customers...")
            logging.info(f"Inserting data into raw.customers...")
            con.sql(f"CREATE TABLE IF NOT EXISTS raw.customers AS SELECT * FROM customers_df")
            con.commit()

            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            logging.info(f"TiendanubeLoader call for getting All Customers has been successfully executed!")

        if self.load_type == "all_abandoned_checkouts":
            logging.info("Executing TiendanubeLoader call for getting All Abandoned Carts...")

            logging.info(f"Executing api calls...")
            abandoned_checkouts = self.get_all_abandoned_checkouts()
            logging.info(f"Creating abandoned_checkouts_df.")
            abandoned_checkouts_df = pd.DataFrame(abandoned_checkouts)

            logging.info("Dataframe abandoned_checkouts_df Columns:")
            logging.info(abandoned_checkouts_df.columns)
            logging.info("Dataframe abandoned_checkouts_df Head(5):")
            logging.info(abandoned_checkouts_df.head(5))

            logging.info(f"Connect to the database...")
            con = self.db.connect()

            logging.info(f"Saving abandoned checkouts data into database...")

            logging.info(f"Creating schema [raw] if not exists...")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS raw;")

            logging.info(f"Creating table raw.abandoned_checkouts...")
            logging.info(f"Inserting data into raw.abandoned_checkouts...")
            con.sql(f"CREATE TABLE IF NOT EXISTS raw.abandoned_checkouts AS SELECT * FROM abandoned_checkouts_df")
            con.commit()

            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            logging.info(f"TiendanubeLoader call for getting All Abandoned Carts has been successfully executed!")

        if self.load_type == "all_products":
            logging.info("Executing TiendanubeLoader call for getting All Products...")

            logging.info(f"Executing api calls...")
            products = self.get_all_products()
            logging.info(f"Creating products_df.")
            products_df = pd.DataFrame(products)

            logging.info("Dataframe products_df Columns:")
            logging.info(products_df.columns)
            logging.info("Dataframe products_df Head(5):")
            logging.info(products_df.head(5))

            logging.info(f"Connect to the database...")
            con = self.db.connect()

            logging.info(f"Saving products data into database...")

            logging.info(f"Creating schema [raw] if not exists...")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS raw;")

            logging.info(f"Creating table raw.products...")
            logging.info(f"Inserting data into raw.products...")
            con.sql(f"CREATE TABLE IF NOT EXISTS raw.products AS SELECT * FROM products_df")
            con.commit()

            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            logging.info(f"TiendanubeLoader call for getting All Products has been successfully executed!")

        if self.load_type == "all_categories":
            logging.info("Executing TiendanubeLoader call for getting All Categories...")

            logging.info(f"Executing api calls...")
            categories = self.get_all_categories()
            logging.info(f"Creating categories_df.")
            categories_df = pd.DataFrame(categories)

            logging.info("Dataframe categories_df Columns:")
            logging.info(categories_df.columns)
            logging.info("Dataframe categories_df Head(5):")
            logging.info(categories_df.head(5))

            logging.info(f"Connect to the database...")
            con = self.db.connect()

            logging.info(f"Saving categories data into database...")

            logging.info(f"Creating schema [raw] if not exists...")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS raw;")

            logging.info(f"Creating table raw.categories...")
            logging.info(f"Inserting data into raw.categories...")
            con.sql(f"CREATE TABLE IF NOT EXISTS raw.categories AS SELECT * FROM categories_df")
            con.commit()

            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            logging.info(f"TiendanubeLoader call for getting All Categories has been successfully executed!")

        if self.load_type == "load_all_orders_in_chunks":

            logging.info("Executing TiendanubeLoader call for getting All Orders...")

            logging.info(f"Executing api calls...")
            self.load_all_orders_in_chunks()

            logging.info(f"TiendanubeLoader call for getting All Orders has been successfully executed!")

    def get_request_json_till_last_page(self, endpoint_name: str, page_n: int,
                                        results_per_page: int = 200) -> Dict:
        """ Get Json content for an API Call

        Returns:
            json
        """

        response = self.api.get(
            endpoint=endpoint_name,
            params={
                'per_page': results_per_page,
                'page': page_n,
            },
            extra_headers={"Content-Type": "application/json"}
        )
        if not response.ok:
            logging.info("No-ok response:")
            logging.info(response)
            if response.status_code == 404:
                logging.info(
                    f"All pages have been successfully processed. The last one is reached!"
                )
                response = {}
                return response
            else:
                logging.error(
                    f"There was an error with API endpoint ({response.status_code})."
                )
                logging.error(f"Response:\n {response.text}")
                raise Exception(f"API Error: error code {response.status_code}")

        return response.json()

    def load_all_orders_in_chunks(self):
        """ Get All Orders from Tiendanube API
            Endpoint: GET / orders
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/order#get-orders
        """

        # Start from the first page
        page_number = 0

        while True:
            orders = []
            logging.info(
                f"Processing orders from Page Number: {page_number}"
            )
            r = self.get_request_json_till_last_page(endpoint_name="orders", page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append parsed orders
            orders.extend(parse_orders(r))

            logging.info(f"Creating orders_df.")
            orders_df = pd.DataFrame(orders)
            logging.info("Dataframe orders_df Columns:")
            logging.info(orders_df.columns)
            logging.info("Dataframe orders_df Head(5):")
            logging.info(orders_df.head(5))

            logging.info(f"Connect to the database...")
            con = self.db.connect()

            logging.info(f"Inserting data into raw.orders...")
            con.sql(f"INSERT INTO raw.orders SELECT * FROM orders_df")
            con.commit()

            logging.info(f"Data saved successfully.")

            logging.info(f"Closing connection to database...")
            self.db.disconnect()
            logging.info(f"Connection closed successfully.")

            # Increment page number for next iteration
            page_number += 1

    def get_all_products(self) -> List[Dict]:
        """ Get All Products from Tiendanube API
            Endpoint: GET / products
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/product
            :return: List of products -> List[Dict]
        """
        products = []
        # Start from the first page
        page_number = 1

        while True:
            logging.info(
                f"Processing products from Page Number: {page_number}"
            )
            r = self.get_request_json_till_last_page(endpoint_name="products", page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append parsed products
            products.extend(parse_products(r))

            # Increment page number for next iteration
            page_number += 1

        return products

    def get_all_customers(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API
            Endpoint: GET / customers
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/customer
            :return: List of customers -> List[Dict]
        """
        customers = []
        # Start from the first page
        page_number = 0

        while True:
            logging.info(
                f"Processing customers from Page Number: {page_number}"
            )
            r = self.get_request_json_till_last_page(endpoint_name="customers", page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append parsed customers
            customers.extend(parse_customers(r))

            # Increment page number for next iteration
            page_number += 1

        return customers

    def get_all_abandoned_checkouts(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API
            Endpoint: GET / abandoned-checkout
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/abandoned-checkout
            :return: List of abandoned-checkout -> List[Dict]
        """
        abandoned_checkouts = []
        # Start from the first page
        page_number = 1

        while True:
            logging.info(
                f"Processing abandoned_checkouts from Page Number: {page_number}"
            )
            r = self.get_request_json_till_last_page(endpoint_name="checkouts", page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append parsed abandoned_checkouts
            abandoned_checkouts.extend(parse_abandoned_checkouts(r))

            # Increment page number for next iteration
            page_number += 1

        return abandoned_checkouts

    def get_all_categories(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API
            Endpoint: GET / category
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/category
            :return: List of abandoned-checkout -> List[Dict]
        """
        categories = []
        # Start from the first page
        page_number = 0

        while True:
            logging.info(
                f"Processing categories from Page Number: {page_number}"
            )
            r = self.get_request_json_till_last_page(endpoint_name="categories", page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append parsed categories
            categories.extend(parse_categories(r))

            # Increment page number for next iteration
            page_number += 1

        return categories
