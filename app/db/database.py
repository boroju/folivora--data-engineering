import duckdb

# Connect to a DuckDB database (or create a new one if it doesn't exist)
con = duckdb.connect(":memory:")

# Define the table schema with column names and data types
sql = """
CREATE TABLE my_table (
  id INTEGER PRIMARY KEY,
  name VARCHAR(255),
  age INTEGER
);
"""

# Execute the SQL statement to create the table
con.execute(sql)

# Close the connection
con.close()

print("Empty table 'my_table' created successfully!")
