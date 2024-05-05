from tiendanube import TiendanubeLoader

tiendanube_loader = TiendanubeLoader(load_type="all_customers")

customers = tiendanube_loader.get_all_customers()

print(customers)

