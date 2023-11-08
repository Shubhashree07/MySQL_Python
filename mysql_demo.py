import mysql.connector
from mysql.connector import Error
import pandas as pd
data1 = [
    ("DeRuyter", 129.74883608352175, 10.0),
    ("Fairbanks", 6389.123328767112, 253.0),
    ("Shannon", 1146.4334629763896, 46.0),
    ("West York", 898.225484719706, 28.0),
    ("Springfield", 2011.3425790883814, 131.0),
    ("Bowling Green", 922.9827230198426, 43.0),
    ("Ryder", 668.923028231959, None),  # Replace 'None' with the appropriate value
]
columns1 = ["City", "TotalSales", "TotalQuantity"]

# Create a Pandas DataFrame
city_sales_df= pd.DataFrame(data1, columns=columns1)





# Define your MySQL connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "mysql@shree987654321",
    "database": "capstone",
}

# Define the DataFrames you want to write to MySQL
#city_sales_df = city_sales.toPandas()
#state_sales_df = state_sales.toPandas()
#source_sales_df = source_sales.toPandas()

try:
    # Create a connection to the MySQL server
    connection = mysql.connector.connect(**db_config)
    
    if connection.is_connected():
        cursor = connection.cursor()

        # Define a function to write a DataFrame to MySQL
        def write_dataframe_to_mysql(df, table_name):
            # Create the table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                City VARCHAR(255),
                TotalSales DECIMAL(10, 2),
                TotalQuantity INT
            )
            """
            cursor.execute(create_table_query)

            # Write the DataFrame to the MySQL table
            for _, row in df.iterrows():
                insert_query = f"""
                INSERT INTO {table_name} (City, TotalSales, TotalQuantity)
                VALUES (%s, %s, %s)
                """
                data = (row["City"], row["TotalSales"], row["TotalQuantity"])
                cursor.execute(insert_query, data)

        # Write DataFrames to MySQL tables
        write_dataframe_to_mysql(city_sales_df, "city_sales")
        #write_dataframe_to_mysql(state_sales_df, "state_sales")
        #write_dataframe_to_mysql(source_sales_df, "source_sales")

        # Commit changes and close the cursor and connection
        connection.commit()
        cursor.close()

except Error as e:
    print(f"Error: {e}")
#finally:
 #   if connection.is_connected():
   #     connection.close()
