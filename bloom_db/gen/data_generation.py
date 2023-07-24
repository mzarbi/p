from random import random, choice

from faker import Faker
import numpy as np
from tqdm import tqdm
import pandas as pd


def generate_data_v3(num_rows=5_000_000):
    fake = Faker()
    np.random.seed(0)

    # Define the unique identifiers for REGION and COUNTRY
    regions = ['EMEA', 'AMER', 'APAC', 'LATAM']
    countries = ['GBR', 'FRA', 'DEU', 'USA', 'AUS', 'IND', 'CHN', 'JPN', 'BRA']
    region_to_countries = {
        'EMEA': ['GBR', 'FRA', 'DEU'],
        'AMER': ['USA'],
        'APAC': ['AUS', 'IND', 'CHN', 'JPN'],
        'LATAM': ['BRA'],
    }

    # Define the banking related columns and their corresponding data generation function
    banking_cols = {
        'Customer_ID': lambda: fake.random_number(digits=10, fix_len=True),
        'Account_Type': lambda: np.random.choice(['Checking', 'Savings', 'Loan', 'Mortgage', 'Credit Card']),
        'Account_Balance': lambda: round(fake.pydecimal(min_value=0, max_value=1000000, right_digits=2), 2),
        'Account_Status': lambda: np.random.choice(['Active', 'Inactive', 'Closed', 'Overdrawn']),
        'Overdraft_Protection': fake.pybool,
        'Loan_Amount': lambda: round(fake.pydecimal(min_value=0, max_value=500000, right_digits=2), 2),
        'Loan_Status': lambda: np.random.choice(['Current', 'In Arrears', 'Default']),
        'Credit_Score': lambda: fake.random_int(min=300, max=850),
        'Fraud_Score': lambda: fake.random_int(min=0, max=100),
        'Last_Login_Date': fake.date_this_decade,
        # add more columns as needed
    }

    # Add date columns
    date_cols = {
        'Last_Login_Date': fake.date_this_decade,
        'Account_Open_Date': lambda: fake.date_between(start_date='-10y', end_date='today'),
        'Last_Transaction_Date': lambda: fake.date_between(start_date='-1y', end_date='today'),
        # add more date columns as needed
    }
    banking_cols.update(date_cols)

    # Populate with additional columns
    for i in range(0, 10):
        banking_cols[f'Bank_Trans_{i}'] = lambda: round(
            fake.pydecimal(min_value=-10000, max_value=10000, right_digits=2), 2)

    # Create random data for other columns
    data_types = [fake.pyint, fake.pyfloat, fake.pybool, fake.word]
    for i in range(10, 30):
        random_data_type = choice(data_types)
        banking_cols[f'col_{i}'] = lambda: random_data_type()

    # Choose random region
    data = {'REGION': np.random.choice(regions, num_rows)}
    # Choose random country based on region
    data['COUNTRY'] = [np.random.choice(region_to_countries[region]) for region in
                       tqdm(data['REGION'], desc="Generating countries")]

    # Generate unique CRDS_CODE
    data['CRDS_CODE'] = [fake.random_number(digits=5, fix_len=True) for _ in
                         tqdm(range(num_rows), desc="Generating CRDS codes")]

    # Create specific data for other columns
    for col, gen_func in banking_cols.items():
        data[col] = [gen_func() for _ in tqdm(range(num_rows), desc=f"Generating values for {col}")]

    df = pd.DataFrame(data)

    df.to_parquet('data_v3.parquet', engine='pyarrow', index=False)


generate_data_v3()
#df = pd.read_parquet("data_v3_5M.parquet")
#print(df.dtypes)
