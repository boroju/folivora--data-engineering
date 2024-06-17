from app.loaders.utils import Dict, List


def parse_orders(orders: Dict) -> List[Dict]:
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
            "customer_id": None,
            "customer_name": None,
            "customer_email": None,
            "customer_identification": None,
            "customer_note": None,
            "customer_last_order_id": None,
            "customer_first_interaction": None,
            "customer_created_at": None,
            "customer_updated_at": None,
            "customer_accepts_marketing": None,
            "customer_accepts_marketing_updated_at": None,
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

        if reg.get("customer"):
            # Update row with default customer details
            row.update({
                "customer_id": reg['customer'].get('id', None),
                "customer_name": reg['customer'].get('name', None),
                "customer_identification": reg['customer'].get('identification', None),
                "customer_email": reg['customer'].get('email', None),
                "customer_note": reg['customer'].get('note', None),
                "customer_last_order_id": reg['customer'].get('last_order_id', None),
                "customer_first_interaction": reg['customer'].get('first_interaction', None),
                "customer_created_at": reg['customer'].get('created_at', None),
                "customer_updated_at": reg['customer'].get('updated_at', None),
                "customer_accepts_marketing": reg['customer'].get('accepts_marketing', None),
                "customer_accepts_marketing_updated_at": reg['customer'].get('accepts_marketing_updated_at', None),
            })

        if reg.get("customer_visit"):
            # Update row with default customer_visit details
            row.update({
                "customer_visit_created_at": reg['customer_visit'].get('created_at', None),
                "customer_visit_landing_page": reg['customer_visit'].get('landing_page', None),
                "customer_visit_utm_parameters_utm_campaign": reg['customer_visit']['utm_parameters'].get(
                    'utm_campaign', None),
                "customer_visit_utm_parameters_utm_content": reg['customer_visit']['utm_parameters'].get('utm_content',
                                                                                                         None),
                "customer_visit_utm_parameters_utm_medium": reg['customer_visit']['utm_parameters'].get('utm_medium',
                                                                                                        None),
                "customer_visit_utm_parameters_utm_source": reg['customer_visit']['utm_parameters'].get('utm_source',
                                                                                                        None),
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
