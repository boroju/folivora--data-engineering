from app.ingestion.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import os

load_dotenv()

products_loader = TiendanubeLoader(load_type="all_products",
                                   api_key=os.getenv('TIENDANUBE_API_KEY'),
                                   api_host=os.getenv('TIENDANUBE_HOST'))

products_loader.load()
