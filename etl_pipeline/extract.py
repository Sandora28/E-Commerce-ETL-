# extract.py

import pandas as pd

def load_data(filename):
    data = pd.read_csv(filename)
    return data

def load_customers_data():
    """Load customer details from a CSV file."""
    customers_df = pd.read_csv('../data/customer_details.csv')
    return customers_df

def load_basket_data():
    """Load basket details from a CSV file."""
    baskets_df = pd.read_csv('../data/basket_details.csv')
    return baskets_df

def load_updated_basket_data():
    """Load basket details from a CSV file."""
    baskets_df = pd.read_csv('../data/updated_basket_details.csv')
    return baskets_df



# Example usage if you want to test this part:
if __name__ == "__main__":
    customers_df = load_customers_data()
    baskets_df = load_basket_data()
    print(customers_df.head())  # Print the first few rows to verify
    print(baskets_df.head())  # Print the first few rows to verify
