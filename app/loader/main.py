from tiendanube import TiendanubeLoader
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Read api specs
api_key = config['api']['key']
api_host = config['api']['host']

tiendanube_loader = TiendanubeLoader(load_type="all_customers", api_key=api_key, host=api_host)

customers = tiendanube_loader.get_all_customers()

print(customers)

