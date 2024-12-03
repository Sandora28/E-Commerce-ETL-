import pandas as pd
import numpy as np
from extract import load_customers_data, load_basket_data, load_updated_basket_data, load_data

# loading data from extract.py
customer_csv = "../data/customer_details.csv"
basket_csv = "../data/basket_details.csv"
customers_df = load_data(customer_csv)
baskets_df = load_data(basket_csv)

# extracting the first 5000 customer_id's from customers_df,
# I need to override customer_id in baskets_df because ids are not matching in two csv files,
# they were all unique
customer_ids_to_use = customers_df['customer_id'].head(5000)

# distribute customer id proportionally to match the number of rows in baskets_df
# this step also handles edge case if size of dataset will be increased
num_baskets = len(baskets_df)
num_customers = len(customer_ids_to_use)
repeat_coefficient = num_baskets // num_customers

# repeat customer id by the coefficient
repeated_customer_ids = np.tile(customer_ids_to_use, repeat_coefficient)
extra_ids_needed = num_baskets - len(repeated_customer_ids)

# if I need extra rows to fill , this will concatenate exact number of rows
if extra_ids_needed > 0:
    repeated_customer_ids = np.concatenate([repeated_customer_ids, customer_ids_to_use[:extra_ids_needed]])

# assign the repeated customer_ids to the baskets_df
baskets_df['customer_id'] = repeated_customer_ids[:num_baskets]

# aggregate basket count per customer
# group by customer_id and sum the basket_count
basket_count = baskets_df.groupby('customer_id')['basket_count'].sum().reset_index(name='total_basket_count')

# merge customer details with their total basket count
full_df = pd.merge(customers_df, basket_count, on='customer_id', how='left')

#define a function to calculate discount based on tenure
def get_sale_percentage(tenure):
    if pd.isna(tenure) or tenure == 0:
        return 0
    elif tenure >= 100:
        return 30
    elif tenure >= 50:
        return 20
    elif tenure >= 10:
        return 10
    else:
        return 5

full_df['tenure'] = full_df['tenure'].fillna(0)

# apply the discount logic to create a new sale column
full_df['sale'] = full_df['tenure'].apply(get_sale_percentage)

# final dataset with only unique customer IDs, total_basket_count, tenure, and sale
final_df = full_df[['customer_id', 'total_basket_count', 'tenure', 'sale']].copy()

# drop any rows with missing values (if any)
final_df.dropna(inplace=True)

# convert the total_basket_count to integer for consistency
final_df['total_basket_count'] = final_df['total_basket_count'].astype(int)

# save the result to a new CSV file
final_df.to_csv('../data/final_transformed_data.csv', index=False)
print(final_df)


