from app.ingestion.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import os

load_dotenv()

abandoned_checkouts_loader = TiendanubeLoader(load_type="all_abandoned_checkouts",
                                              api_key=os.getenv('TIENDANUBE_API_KEY'),
                                              api_host=os.getenv('TIENDANUBE_HOST'))

abandoned_checkouts_loader.load()
