from app.loaders.tiendanube import TiendanubeLoader
from dotenv import load_dotenv
import pandas as pd
import duckdb
import os

load_dotenv()

tiendanube_loader = TiendanubeLoader(load_type="all_customers",
                                     api_key=os.getenv('TIENDANUBE_API_KEY'),
                                     api_host=os.getenv('TIENDANUBE_HOST'))

customers = tiendanube_loader.get_all_customers()

customers_df = pd.DataFrame(customers)

print("Dataframe info:")
print(customers_df.info())

print("Dataframe DTypes:")
data_types = customers_df.dtypes
print(data_types)

print("Dataframe Columns:")
column_names = customers_df.columns
print(column_names)

number_of_rows = len(customers_df)
print(f"The DataFrame has {number_of_rows} rows.")

# Connect to a DuckDB database (or create a new one if it doesn't exist)
con = duckdb.connect("my_database.duckdb")

# create the table "my_table" from the DataFrame "my_customers_df"
con.sql("CREATE TABLE IF NOT EXISTS customers AS SELECT * FROM customers_df")

# insert into the table "my_table" from the DataFrame "my_customers_df"
con.sql("INSERT INTO customers SELECT * FROM customers_df")

con.commit()  # Commit changes to the database

# Close the connection (optional if using an in-memory connection)
con.close()

print("Customer table created successfully!")