from app.loaders.utils import Dict, List


def parse_products(products: Dict) -> List[Dict]:
    """
    Parse products
    :param products: Dict
    :return: List of products -> List[Dict]
    """
    rows = []
    for reg in products:
        row = {
            "id": reg.get('id', None),
            "name_es": reg['name'].get('es', None),
            "description_es": reg['description'].get('es', None),
            "handle_es": reg['handle'].get('es', None),
            "seo_title_es": reg['seo_title'].get('es', None),
            "seo_description_es": reg['seo_description'].get('es', None),
            "attributes": reg.get('attributes', None),
            "published": reg.get('published', None),
            "free_shipping": reg.get('free_shipping', None),
            "requires_shipping": reg.get('requires_shipping', None),
            "canonical_url": reg.get('canonical_url', None),
            "video_url": reg.get('video_url', None),
            "brand": reg.get('brand', None),
            "created_at": reg.get('created_at', None),
            "updated_at": reg.get('updated_at', None),
            "variants": reg.get('variants', None),
            "variants_id_last": None,
            "variants_product_id_last": None,
            "variants_position_last": None,
            "variants_price_last": None,
            "variants_compare_at_price_last": None,
            "variants_promotional_price_last": None,
            "variants_stock_management_last": None,
            "variants_stock_last": None,
            "variants_sku_last": None,
            "variants_barcode_last": None,
            "tags": reg.get('tags', None),
            "images": reg.get('images', None),
            "categories": reg.get('categories', None),
        }

        if reg.get("variants"):
            # Update row with default address details
            row.update({
                "variants_id_last": reg['variants'][0].get('id', None),
                "variants_product_id_last": reg['variants'][0].get('product_id', None),
                "variants_position_last": reg['variants'][0].get('position', None),
                "variants_price_last": reg['variants'][0].get('price', None),
                "variants_compare_at_price_last": reg['variants'][0].get('compare_at_price', None),
                "variants_promotional_price_last": reg['variants'][0].get('promotional_price', None),
                "variants_stock_management_last": reg['variants'][0].get('stock_management', None),
                "variants_stock_last": reg['variants'][0].get('stock', None),
                "variants_sku_last": reg['variants'][0].get('sku', None),
                "variants_barcode_last": reg['variants'][0].get('barcode', None),
            })

        rows.append(row)
    return rows
