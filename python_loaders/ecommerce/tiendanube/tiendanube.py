import pandas as pd
import json
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
        Documentation:
            https://tiendanube.github.io/api-documentation/intro
        """
        super(TiendanubeLoader, self).__init__(*args, **kwargs)

        # Dates to a "%Y%m%d"
        self.processed_date = str(datetime.now().date().strftime("%Y%m%d"))

        # Date Today ISO Format
        self.date_today_iso = datetime.now().date().isoformat()
        logging.info(self.date_today_iso)

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
            logging.info("Storing data in " + customers_csv)
            df.to_csv(customers_csv, index=False)

            logging.info("Data Printed.")
        else:
            logging.critical("No data.")

    def get_request_customers_created_after_date_json(self):
        """ Get Json content for an API Call

        Returns:
            json
        """

        response = self.api.get(
            endpoint=self.config["CUSTOMERS_ENDPOINT"],
            # Show Customers created after date (ISO 8601 format)
            params={
                'created_at_min': self.date_today_iso
            },
            extra_headers={"Content-Type": "application/json"}
        )
        if not response.ok:
            logging.error(
                f"There was an error with API endpoint ({response.status_code})."
            )
            logging.error(f"Response:\n {response.text}")
            raise Exception(f"API Error: error code {response.status_code}")

        return response.json()

    def get_request_all_customers_json(self, page_number: int):
        """ Get Json content for an API Call

        Returns:
            json
        """
        last_page = False
        response = self.api.get(
            endpoint=self.config["CUSTOMERS_ENDPOINT"],
            params={
                # Amount of results - max per page is 200
                'per_page': 200,
                'page': page_number
            },
            extra_headers={"Content-Type": "application/json"}
        )
        if not response.ok:
            response_json = json.loads(response.text)
            if response.status_code == 404 \
                    and 'description' in response_json \
                    and 'Last page' in response_json['description']:
                last_page = True
                logging.info("Reaching last page of Customers!")
                logging.info("Last page is:" + str(page_number))
            else:
                logging.error(
                    f"There was an error with API endpoint ({response.status_code})."
                )
                logging.error(f"Response:\n {response.text}")
                raise Exception(f"API Error: error code {response.status_code}")

        logging.info("Page number: " + str(page_number))
        return response.json(), last_page

    def get_all_customers(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API """
        rows = []
        last_page = False
        page = 1
        while not last_page:
            r, last_page = self.get_request_all_customers_json(page_number=page)
            if not last_page:
                logging.info("Appending more customers...")
                rows.append(self.parse_all_customers(r))
            page = page + 1

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

