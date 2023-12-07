import csv
import psycopg2
import pandas as pd

def all_location_postgre() -> pd.DataFrame :
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')
        
    # Create a cursor object
    cur = conn.cursor()

    # Fetch new data from the table where id is greater than max_id
    #cur.execute("SELECT DISTINCT location FROM main_jobs")
    cur.execute("SELECT DISTINCT location FROM test")

    data = cur.fetchall()

    # Create a DataFrame with fetched data and set column names
    df = pd.DataFrame(data, columns=["location"])

    # Close the database connection
    cur.close()
    conn.close()

    return df

df = all_location_postgre()

df.to_csv("/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/download/sql_distinct.csv")

print(f"Data exported to {df}")


