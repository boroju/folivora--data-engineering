from utils import BearerAuthApi
from utils import logging
from utils import Dict, List
from datetime import datetime


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

    def load(self):

        if self.load_type == "all_customers":

            logging.info("Executing Tiendanube Api call for getting All Customers...")

            # Goes to file: all_customers.csv
            data = {
                "customers": self.get_all_customers()
            }

        else:
            data = {}

        # Check if data exists
        if len(data.items()) > 0:
            # Persist data if needed
            logging.info("There is data.")
        else:
            logging.critical("No data.")

    def get_customers_request_json(self, page_n: int, results_per_page: int = 200) -> Dict:
        """ Get Json content for an API Call

        Returns:
            json
        """

        response = self.api.get(
            endpoint="customers",
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
                    f"All customers have been successfully processed. Last page is reached!"
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

    def get_all_customers(self) -> List[Dict]:
        """ Get All Customers from Tiendanube API """
        customers = []
        # Start from the first page
        page_number = 0

        while True:
            logging.info(
                f"Processing customers from Page Number: {page_number}"
            )
            r = self.get_customers_request_json(page_n=page_number)
            # Check if response is empty (including last page case)
            if not r:
                logging.info(
                    f"Reached the last page."
                )
                break

            # Append customers without parsing
            # customers.extend(r)
            # Append parsed customers
            customers.extend(self.parse_customers(r))

            # Increment page number for next iteration
            page_number += 1

        return customers

    def parse_customers(self, customers):
        rows = []
        for reg in customers:
            row = {
                "id": reg.get('id', None),
                "name": reg.get('name', None),
                "email": reg.get('email', None),
                "identification": reg.get('identification', None),
                "phone": reg.get('phone', None),
                "total_spent": reg.get('total_spent', None),
                "total_spent_currency": reg.get('total_spent_currency', None),
                "active": reg.get('active', None),
                "last_order_id": reg.get('last_order_id', None),
                "first_interaction": reg.get('first_interaction', None),
                "created_at": reg.get('created_at', None),
                "updated_at": reg.get('updated_at', None),
                "default_address_street": None,
                "default_address_number": None,
                "default_address_floor": None,
                "default_address_city": None,
                "default_address_country": None,
                "default_address_created_at": None,
                "default_address_id": None,
                "default_address_locality": None,
                "default_address_province": None,
                "default_address_updated_at": None,
                "default_address_zipcode": None,
                "billing_country": reg.get('billing_country', None),
                "accepts_marketing": reg.get('accepts_marketing', None),
                "accepts_marketing_updated_at": reg.get('accepts_marketing_updated_at', None),
                "processed_date": self.processed_date
            }

            if reg.get("default_address"):
                # Update row with default address details
                row.update({
                    "default_address_street": reg['default_address'].get('address', None),
                    "default_address_number": reg['default_address'].get('number', None),
                    "default_address_floor": reg['default_address'].get('floor', None),
                    "default_address_city": reg['default_address'].get('city', None),
                    "default_address_country": reg['default_address'].get('country', None),
                    "default_address_created_at": reg['default_address'].get('created_at', None),
                    "default_address_id": reg['default_address'].get('id', None),
                    "default_address_locality": reg['default_address'].get('locality', None),
                    "default_address_province": reg['default_address'].get('province', None),
                    "default_address_updated_at": reg['default_address'].get('updated_at', None),
                    "default_address_zipcode": reg['default_address'].get('zipcode', None),
                })

            rows.append(row)
        return rows
