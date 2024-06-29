from app.loaders.utils import Dict, List


def parse_categories(categories: Dict) -> List[Dict]:
    """
    Parse categories
    :param categories: Dict
    :return: List of categories -> List[Dict]
    """
    rows = []
    for reg in categories:
        row = {
            "id": reg.get('id', None),
            "parent": reg.get('parent', None),
            "subcategories": reg.get('subcategories', None),
            "google_shopping_category": reg.get('google_shopping_category', None),
            "created_at": reg.get('created_at', None),
            "updated_at": reg.get('updated_at', None),
            "name_es": reg['name'].get('es', None),
            "handle_es": reg['handle'].get('es', None),
            "description_es": reg['description'].get('es', None),
            "seo_title_es": reg['seo_title'].get('es', None),
            "seo_description_es": reg['seo_description'].get('es', None),
        }

        rows.append(row)
    return rows
