import pandas as pd
from python_loaders.base import BasePythonLoader
from python_loaders.utils import BearerAuthApi, logging
from datetime import datetime
from typing import Dict, List

class TiendanubeLoader(BasePythonLoader):
    name = "tiendanube"

    def __init__(self,
                 load_type: str = None,
                 *args,
                 **kwargs):
        """ Init function to build TiendanubeLoader
        Args:
            load_type: with the possible following options to load data --> all_customers or all_orders
        Returns:
            Nothing
        """
        super(TiendanubeLoader, self).__init__(*args, **kwargs)

        # Dates to a "%Y%m%d"
        self.processed_date = str(datetime.now().date().strftime("%Y%m%d"))

        # create api auth
        api_key = self.config["API_KEY"]
        api_host = self.config["API_HOST"]

        # Api Authorization
        self.api = BearerAuthApi(
            api_key=api_key,
            host=api_host
        )
        self.load_type = load_type

    def load(self):

        if self.load_type == "all_customers":

            logging.info("Executing Tiendanube Api call for getting All Customers...")

            # Goes to file: all_customers.csv
            data = {
                "customers": self.get_all_customers()
            }

        else:
            data = {}

        customers_csv = self.config["CUSTOMERS_CSV"]
        # Check if data exists
        if len(data.items()) > 0:
            df = pd.DataFrame.from_dict(data["customers"])
            print("Storing data in " + customers_csv)
            df.to_csv(customers_csv, index=False)

            logging.info("Data Printed.")
        else:
            logging.critical("No data.")

    def get_request_all_customers_json(self):
        """ Get Json content for an API Call

        Returns:
            json
        """

        response = self.api.get(
            endpoint=self.config["CUSTOMERS_ENDPOINT"],
            params={},
            extra_headers={"Content-Type": "application/json"}
        )
        if not response.ok:
            logging.error(
                f"There was an error with API endpoint ({response.status_code})."
            )
            logging.error(f"Response:\n {response.text}")
            raise Exception(f"API Error: error code {response.status_code}")

        return response.json()

    def get_all_customers(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API """
        r = self.get_request_all_customers_json()

        rows = self.parse_all_customers(r)
        return rows

    def parse_all_customers(self, customers):
        rows = []
        for reg in customers:
            row = {
                "id": reg.get('id'),
                "name": reg.get('name'),
                "email": reg.get('email'),
                "identification": reg.get('identification'),
                "phone": reg.get('phone'),
                "total_spent": reg.get('total_spent'),
                "total_spent_currency": reg.get('total_spent_currency'),
                "active": reg.get('active'),
                "last_order_id": reg.get('last_order_id'),
                "first_interaction": reg.get('first_interaction'),
                "created_at": reg.get('created_at'),
                "updated_at": reg.get('updated_at'),
                "processed_date": self.processed_date
            }
            rows.append(row)
        return rows

