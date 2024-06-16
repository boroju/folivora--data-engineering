from app.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import pandas as pd
import duckdb
import os

load_dotenv()

tiendanube_loader = TiendanubeLoader(load_type="all_orders",
                                     api_key=os.getenv('TIENDANUBE_API_KEY'),
                                     api_host=os.getenv('TIENDANUBE_HOST'))

orders = tiendanube_loader.get_all_orders()

orders_df = pd.DataFrame(orders)

print("Dataframe info:")
print(orders_df.info())

print("Dataframe DTypes:")
data_types = orders_df.dtypes
print(data_types)

print("Dataframe Columns:")
column_names = orders_df.columns
print(column_names)

number_of_rows = len(orders_df)
print(f"The DataFrame has {number_of_rows} rows.")

print("Orders DataFrame:")
print(orders_df.head(3))

# Connect to a DuckDB database (or create a new one if it doesn't exist)
con = duckdb.connect("my_database.duckdb")

# create the table "orders" from the DataFrame "orders_df"
con.sql("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM orders_df")

# insert into the table "orders" from the DataFrame "orders_df"
con.sql("INSERT INTO orders SELECT * FROM orders_df")

con.commit()  # Commit changes to the database

# Close the connection (optional if using an in-memory connection)
con.close()

print("Orders table created successfully!")