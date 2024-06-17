from app.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import os

load_dotenv()

orders_loader = TiendanubeLoader(load_type="all_orders",
                                 api_key=os.getenv('TIENDANUBE_API_KEY'),
                                 api_host=os.getenv('TIENDANUBE_HOST'))

orders_loader.load()
