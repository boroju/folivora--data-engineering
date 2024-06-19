from app.loaders.utils import Dict, List


def parse_abandoned_checkouts(abandoned_checkouts: Dict) -> List[Dict]:
    """
    Parse abandoned_checkouts
    :param abandoned_checkouts: Dict
    :return: List of abandoned_checkouts -> List[Dict]
    """
    rows = []
    for reg in abandoned_checkouts:
        row = {
            "id": reg.get('id', None),
            "token": reg.get('token', None),
            "store_id": reg.get('store_id', None),
            "abandoned_checkout_url": reg.get('abandoned_checkout_url', None),
            "contact_email": reg.get('contact_email', None),
            "contact_name": reg.get('contact_name', None),
            "contact_phone": reg.get('contact_phone', None),
            "contact_identification": reg.get('contact_identification', None),
            "shipping_name": reg.get('shipping_name', None),
            "shipping_phone": reg.get('shipping_phone', None),
            "shipping_address": reg.get('shipping_address', None),
            "shipping_number": reg.get('shipping_number', None),
            "shipping_floor": reg.get('shipping_floor', None),
            "shipping_locality": reg.get('shipping_locality', None),
            "shipping_zipcode": reg.get('shipping_zipcode', None),
            "shipping_city": reg.get('shipping_city', None),
            "shipping_province": reg.get('shipping_province', None),
            "shipping_country": reg.get('shipping_country', None),
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
            "shipping_suboption": reg.get('shipping_suboption', None),
            "storefront": reg.get('storefront', None),
            "note": reg.get('note', None),
            "created_at": reg.get('created_at', None),
            "updated_at": reg.get('updated_at', None),
            "completed_at": reg.get('completed_at', None),
            "next_action": reg.get('next_action', None),
            "payment_details_method": None,
            "payment_details_credit_card_company": None,
            "payment_details_installments": None,
            "free_shipping_config": reg.get('free_shipping_config', None),
            "payment_count": reg.get('payment_count', None),
            "payment_status": reg.get('payment_status', None),
            "order_origin": reg.get('order_origin', None),
            "same_billing_and_shipping_address": reg.get('same_billing_and_shipping_address', None),
            "total_paid": reg.get('total_paid', None),
            "customer": reg.get('customer', None),
            "contact_accepts_marketing": reg.get('contact_accepts_marketing', None),
            "contact_accepts_marketing_updated_at": reg.get('contact_accepts_marketing_updated_at', None),
            "products": reg.get('products', None),
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

        if reg.get("payment_details"):
            # Update row with default payment_details details
            row.update({
                "payment_details_method": reg['payment_details'].get('method', None),
                "payment_details_credit_card_company": reg['payment_details'].get('credit_card_company', None),
                "payment_details_installments": reg['payment_details'].get('installments', None),
            })

        rows.append(row)
    return rows
