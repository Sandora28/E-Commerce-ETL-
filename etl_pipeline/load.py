import sqlite3
import pandas as pd
from extract import load_updated_basket_data
from sqlalchemy import create_engine


def load_data_to_database(final_df, db_name='../data/ecommerce_data.db'):
    try:
        conn = sqlite3.connect(db_name)
        final_df.to_sql('finalized_data', conn, if_exists='replace', index=False)
        print(f"Data successfully loaded into {db_name}")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        if conn:
            conn.close()



def load_data_to_postgresql(final_df, table_name,db_name="ecommerce_data", user="ecommerce_user", password="ecommerce_user", host='localhost', port=5432):
    try:
        conn_string = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(conn_string)
        with engine.begin() as connection:  # Using begin() to manage transactions
            final_df.to_sql(table_name, connection, if_exists='replace', index=False)
        print(f"Data successfully loaded into {db_name} database, table '{table_name}'")

    except Exception as e:
        print(f"An error occurred: {e}")




final_df = pd.read_csv('../data/final_transformed_data.csv')
basket_for_post =load_updated_basket_data()

load_data_to_postgresql(final_df,"finalized_data")
load_data_to_postgresql(basket_for_post,"basket")
load_data_to_database(final_df)
