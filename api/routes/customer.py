from fastapi import APIRouter, HTTPException
from data.customer_collection import CustomerCollection
from loaders.tiendanube import TiendanubeLoader
from api.models.customer import Customer
from utils import find_project_root_path
from api.schema.schemas import customers_serial

# Read config file
config_file = find_project_root_path() + '/config.ini'

# Initialize customer_collection
customer_collection = CustomerCollection(config_file=config_file)

customer = APIRouter()


@customer.get("/customer", response_model=list[Customer], tags=["customer"])
async def get_customers():
    # Initialize customer_collection
    customers = customers_serial(customer_collection.find())
    return customers


# Endpoint to refresh all customers from Tiendanube API and store them in MongoDB database
@customer.post("/customer/refresh")
async def refresh_customers_on_db():
    # Initialize TiendanubeLoader
    tiendanube_api = TiendanubeLoader(api_key=config['tiendanube']['api_key'],
                                      api_host=config['tiendanube']['host'],
                                      load_type="all_customers")

    # Fetch all customers from TiendanubeLoader
    customers = tiendanube_api.get_all_customers()

    if not customers:
        raise HTTPException(status_code=500, detail="Failed to fetch customers from Tiendanube API")

    # Delete existing customers in MongoDB collection
    customer_collection.delete_many({})

    # Insert customers into MongoDB collection
    customer_collection.insert_many(customers)

    return {"message": "Customers refreshed successfully!"}

# Example usage:
# This endpoint can be triggered by sending a POST request to "/refresh_all_customers"
