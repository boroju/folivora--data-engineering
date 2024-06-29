from app.ingestion.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import os

load_dotenv()

orders_loader = TiendanubeLoader(load_type="load_all_orders_in_chunks",
                                 api_key=os.getenv('TIENDANUBE_API_KEY'),
                                 api_host=os.getenv('TIENDANUBE_HOST'))

orders_loader.load()
