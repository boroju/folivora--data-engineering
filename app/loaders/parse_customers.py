from app.loaders.utils import Dict, List


def parse_customers(customers: Dict) -> List[Dict]:
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
