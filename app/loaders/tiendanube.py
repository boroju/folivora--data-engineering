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

            data = {
                "customers": self.get_all_customers()
            }

        if self.load_type == "all_orders":

            logging.info("Executing Tiendanube Api call for getting All Orders...")

            data = {
                "orders": self.get_all_orders()
            }

        else:
            data = {}

        # Check if data exists
        if len(data.items()) > 0:
            # Persist data if needed
            logging.info("There is data.")
        else:
            logging.critical("No data.")

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

    def get_all_orders(self) -> List[Dict]:
        """ Get All Orders from Tiendanube API
            Endpoint: GET / orders
            - Api documentation: https://tiendanube.github.io/api-documentation/resources/order#get-orders
            :return: List of orders -> List[Dict]
        """
        orders = []
        # Start from the first page
        page_number = 0

        # TODO: Remove comment from while True
        # while True:
        for i in range(0, 1):
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
            orders.extend(self.parse_orders(r))

            # Increment page number for next iteration
            page_number += 1

        return orders

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
            customers.extend(self.parse_customers(r))

            # Increment page number for next iteration
            page_number += 1

        return customers

    def parse_customers(self, customers: Dict) -> List[Dict]:
        """
        Parse customers
        :param customers: Dict
        :return: List of customers -> List[Dict]
        """
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

    def parse_orders(self, orders: Dict):
        """
        Parse orders
        :param orders: Dict
        :return: List of orders -> List[Dict]
        """
        rows = []
        for reg in orders:
            row = {
                "id": reg.get('id', None),
                "token": reg.get('token', None),
                "store_id": reg.get('store_id', None),
                "contact_email": reg.get('contact_email', None),
                "contact_name": reg.get('contact_name', None),
                "contact_phone": reg.get('contact_phone', None),
                "contact_identification": reg.get('contact_identification', None),
                "shipping_min_days": reg.get('shipping_min_days', None),
                "shipping_max_days": reg.get('shipping_max_days', None),
                "billing_name": reg.get('billing_name', None),
                "billing_phone": reg.get('billing_phone', None),
                "billing_address": reg.get('billing_address', None),
                "billing_number": reg.get('billing_number', None),
                "billing_floor": reg.get('billing_floor', None),
                "billing_locality": reg.get('billing_locality', None),
                "billing_zipcode": reg.get('billing_zipcode', None),
                "billing_city": reg.get('billing_city', None),
                "billing_province": reg.get('billing_province', None),
                "billing_country": reg.get('billing_country', None),
                "billing_customer_type": reg.get('billing_customer_type', None),
                "billing_business_name": reg.get('billing_business_name', None),
                "billing_trade_name": reg.get('billing_trade_name', None),
                "billing_state_registration": reg.get('billing_state_registration', None),
                "billing_document_type": reg.get('billing_document_type', None),
                "shipping_cost_owner": reg.get('shipping_cost_owner', None),
                "shipping_cost_customer": reg.get('shipping_cost_customer', None),
                "coupon_id": None,
                "coupon_code": None,
                "coupon_type": None,
                "coupon_value": None,
                "coupon_valid": None,
                "coupon_used": None,
                "coupon_max_uses": None,
                "coupon_includes_shipping": None,
                "coupon_start_date": None,
                "coupon_end_date": None,
                "coupon_min_price": None,
                "coupon_categories": None,
                "promotional_discount_id": None,
                "promotional_discount_store_id": None,
                "promotional_discount_order_id": None,
                "promotional_discount_created_at": None,
                "promotional_discount_total_discount_amount": None,
                "subtotal": reg.get('subtotal', None),
                "discount": reg.get('discount', None),
                "discount_coupon": reg.get('discount_coupon', None),
                "discount_gateway": reg.get('discount_gateway', None),
                "total": reg.get('total', None),
                "total_usd": reg.get('total_usd', None),
                "checkout_enabled": reg.get('checkout_enabled', None),
                "weight": reg.get('weight', None),
                "currency": reg.get('currency', None),
                "language": reg.get('language', None),
                "gateway": reg.get('gateway', None),
                "gateway_id": reg.get('gateway_id', None),
                "gateway_name": reg.get('gateway_name', None),
                "shipping": reg.get('shipping', None),
                "shipping_option": reg.get('shipping_option', None),
                "shipping_option_code": reg.get('shipping_option_code', None),
                "shipping_option_reference": reg.get('shipping_option_reference', None),
                "shipping_pickup_details": reg.get('shipping_pickup_details', None),
                "shipping_tracking_number": reg.get('shipping_tracking_number', None),
                "shipping_tracking_url": reg.get('shipping_tracking_url', None),
                "shipping_store_branch_name": reg.get('shipping_store_branch_name', None),
                "shipping_store_branch_extra": reg.get('shipping_store_branch_extra', None),
                "shipping_pickup_type": reg.get('shipping_pickup_type', None),
                "storefront": reg.get('storefront', None),
                "created_at": reg.get('created_at', None),
                "updated_at": reg.get('updated_at', None),
                "completed_at_date": None,
                "completed_at_timezone_type": None,
                "completed_at_timezone": None,
                "next_action": reg.get('next_action', None),
                "payment_details_method": None,
                "payment_details_credit_card_company": None,
                "payment_details_installments": None,
                "free_shipping_config": reg.get('free_shipping_config', None),
                "payment_count": reg.get('payment_count', None),
                "payment_status": reg.get('payment_status', None),
                "order_origin": reg.get('order_origin', None),
                "same_billing_and_shipping_address": reg.get('same_billing_and_shipping_address', None),
                "last_order_id": reg.get('last_order_id', None),
                "first_interaction": reg.get('first_interaction', None),
                "created_at": reg.get('created_at', None),
                "updated_at": reg.get('updated_at', None),
                "accepts_marketing": reg.get('accepts_marketing', None),
                "accepts_marketing_updated_at": reg.get('accepts_marketing_updated_at', None),
                # TODO: Check if this retrieves a list of products
                "products": reg.get('products', None),
                # From where the customer came from
                "customer_visit_created_at": None,
                "customer_visit_landing_page": None,
                "customer_visit_utm_parameters_utm_campaign": None,
                "customer_visit_utm_parameters_utm_content": None,
                "customer_visit_utm_parameters_utm_medium": None,
                "customer_visit_utm_parameters_utm_source": None,
                "customer_visit_utm_parameters_utm_term": None,
                "fulfillments": reg.get('fulfillments', None),
                "number": reg.get('number', None),
                "cancel_reason": reg.get('cancel_reason', None),
                "owner_note": reg.get('owner_note', None),
                "cancelled_at": reg.get('cancelled_at', None),
                "closed_at": reg.get('closed_at', None),
                "read_at": reg.get('read_at', None),
                "status": reg.get('status', None),
                "gateway_link": reg.get('gateway_link', None),
                "has_shippable_products": reg.get('has_shippable_products', None),
                "shipping_carrier_name": reg.get('shipping_carrier_name', None),
                "shipping_address_address": None,
                "shipping_address_city": None,
                "shipping_address_country": None,
                "shipping_address_created_at": None,
                "shipping_address_default": None,
                "shipping_address_floor": None,
                "shipping_address_id": None,
                "shipping_address_locality": None,
                "shipping_address_name": None,
                "shipping_address_number": None,
                "shipping_address_phone": None,
                "shipping_address_province": None,
                "shipping_address_updated_at": None,
                "shipping_address_zipcode": None,
                "shipping_status": reg.get('shipping_status', None),
                "shipped_at": reg.get('shipped_at', None),
                "paid_at": reg.get('paid_at', None),
                "landing_url": reg.get('landing_url', None),
                "client_details_browser_ip": None,
                "client_details_user_agent": None,
                "app_id": reg.get('app_id', None),
                "processed_date": self.processed_date
            }

            # Check if there's at least one coupon
            if len(reg['coupon']) > 0:
                # Update row with default coupon details
                row.update({
                    "coupon_id": reg['coupon'][0].get('id', None),
                    "coupon_code": reg['coupon'][0].get('code', None),
                    "coupon_type": reg['coupon'][0].get('type', None),
                    "coupon_value": reg['coupon'][0].get('value', None),
                    "coupon_valid": reg['coupon'][0].get('valid', None),
                    "coupon_used": reg['coupon'][0].get('used', None),
                    "coupon_max_uses": reg['coupon'][0].get('max_uses', None),
                    "coupon_includes_shipping": reg['coupon'][0].get('includes_shipping', None),
                    "coupon_start_date": reg['coupon'][0].get('start_date', None),
                    "coupon_end_date": reg['coupon'][0].get('end_date', None),
                    "coupon_min_price": reg['coupon'][0].get('min_price', None),
                    "coupon_categories": reg['coupon'][0].get('categories', None),
                })

            if reg.get("promotional_discount"):
                # Update row with default promotional_discount details
                row.update({
                    "promotional_discount_id": reg['promotional_discount'].get('id', None),
                    "promotional_discount_store_id": reg['promotional_discount'].get('store_id', None),
                    "promotional_discount_order_id": reg['promotional_discount'].get('order_id', None),
                    "promotional_discount_created_at": reg['promotional_discount'].get('created_at', None),
                    "promotional_discount_total_discount_amount": reg['promotional_discount'].get(
                        'total_discount_amount', None),
                })

            if reg.get("completed_at"):
                # Update row with default completed_at details
                row.update({
                    "completed_at_date": reg['completed_at'].get('date', None),
                    "completed_at_timezone_type": reg['completed_at'].get('timezone_type', None),
                    "completed_at_timezone": reg['completed_at'].get('timezone', None),
                })

            if reg.get("payment_details"):
                # Update row with default payment_details details
                row.update({
                    "payment_details_method": reg['payment_details'].get('method', None),
                    "payment_details_credit_card_company": reg['payment_details'].get('credit_card_company', None),
                    "payment_details_installments": reg['payment_details'].get('installments', None),
                })

            if reg.get("customer_visit"):
                # Update row with default customer_visit details
                row.update({
                    "customer_visit_created_at": reg['customer_visit'].get('created_at', None),
                    "customer_visit_landing_page": reg['customer_visit'].get('landing_page', None),
                    "customer_visit_utm_parameters_utm_campaign": reg['customer_visit']['utm_parameters'].get('utm_campaign', None),
                    "customer_visit_utm_parameters_utm_content": reg['customer_visit']['utm_parameters'].get('utm_content', None),
                    "customer_visit_utm_parameters_utm_medium": reg['customer_visit']['utm_parameters'].get('utm_medium', None),
                    "customer_visit_utm_parameters_utm_source": reg['customer_visit']['utm_parameters'].get('utm_source', None),
                    "customer_visit_utm_parameters_utm_term": reg['customer_visit']['utm_parameters'].get('utm_term', None),
                })

            if reg.get("shipping_address"):
                # Update row with default shipping_address details
                row.update({
                    "shipping_address_address": reg['shipping_address'].get('address', None),
                    "shipping_address_city": reg['shipping_address'].get('city', None),
                    "shipping_address_country": reg['shipping_address'].get('country', None),
                    "shipping_address_created_at": reg['shipping_address'].get('created_at', None),
                    "shipping_address_default": reg['shipping_address'].get('default', None),
                    "shipping_address_floor": reg['shipping_address'].get('floor', None),
                    "shipping_address_locality": reg['shipping_address'].get('locality', None),
                    "shipping_address_name": reg['shipping_address'].get('name', None),
                    "shipping_address_number": reg['shipping_address'].get('number', None),
                    "shipping_address_phone": reg['shipping_address'].get('phone', None),
                    "shipping_address_province": reg['shipping_address'].get('province', None),
                    "shipping_address_updated_at": reg['shipping_address'].get('updated_at', None),
                    "shipping_address_zipcode": reg['shipping_address'].get('zipcode', None),
                })

            if reg.get("client_details"):
                # Update row with default client_details details
                row.update({
                    "client_details_browser_ip": reg['client_details'].get('browser_ip', None),
                    "client_details_user_agent": reg['client_details'].get('user_agent', None),
                })

            rows.append(row)
        return rows
