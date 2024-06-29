from app.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import os

load_dotenv()

categories_loader = TiendanubeLoader(load_type="all_categories",
                                     api_key=os.getenv('TIENDANUBE_API_KEY'),
                                     api_host=os.getenv('TIENDANUBE_HOST'))

categories_loader.load()
