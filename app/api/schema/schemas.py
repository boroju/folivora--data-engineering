from app.api.models.customer import Customer
from typing import Dict, List


def customer_serial(customer: Customer) -> Dict:
    return customer.dict()


def customers_serial(customers: List[Customer]) -> List[Dict]:
    return [customer_serial(customer) for customer in customers]
