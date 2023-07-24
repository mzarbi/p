import os
import glob
import pandas as pd
import numpy as np
from faker import Faker
from pybloom_live import BloomFilter

# Function to generate the data
from tqdm import trange
import pandas as pd
import numpy as np
from faker import Faker
import random
from tqdm import tqdm
import pandas as pd
import numpy as np
from faker import Faker
import random


def generate_data_v2(num_rows=50000000, num_cols=50):
    fake = Faker()

    # Define the unique identifiers for REGION and COUNTRY
    regions = ['EMEA', 'AMER', 'APAC', 'LATAM']
    countries = ['GBR', 'FRA', 'DEU', 'USA', 'AUS', 'IND', 'CHN', 'JPN', 'BRA']
    region_to_countries = {
        'EMEA': ['GBR', 'FRA', 'DEU'],
        'AMER': ['USA'],
        'APAC': ['AUS', 'IND', 'CHN', 'JPN'],
        'LATAM': ['BRA'],
    }
    data_types = [fake.pyint, fake.pyfloat, fake.pybool, fake.word]

    # Choose random region
    data = {
        'REGION': np.random.choice(regions, num_rows)
    }
    # Choose random country based on region
    data['COUNTRY'] = [np.random.choice(region_to_countries[region]) for region in data['REGION']]
    # Generate unique CRDS_CODE
    data['CRDS_CODE'] = [fake.unique.random_number(digits=5, fix_len=True) for _ in range(num_rows)]

    # Create random data for other columns
    for i in range(3, num_cols + 1):
        random_data_type = random.choice(data_types)
        data[f'col_{i}'] = [random_data_type() for _ in range(num_rows)]

    df = pd.DataFrame(data)

    df.to_parquet('data_v2.parquet', engine='pyarrow', index=False)


def generate_data_v2(num_rows=5000000, num_cols=50):
    fake = Faker()

    # Define the unique identifiers for REGION and COUNTRY
    regions = ['EMEA', 'AMER', 'APAC', 'LATAM']
    countries = ['GBR', 'FRA', 'DEU', 'USA', 'AUS', 'IND', 'CHN', 'JPN', 'BRA']
    region_to_countries = {
        'EMEA': ['GBR', 'FRA', 'DEU'],
        'AMER': ['USA'],
        'APAC': ['AUS', 'IND', 'CHN', 'JPN'],
        'LATAM': ['BRA'],
    }
    data_types = [fake.pyint, fake.pyfloat, fake.pybool, fake.word]

    # Choose random region
    data = {
        'REGION': np.random.choice(regions, num_rows)
    }
    # Choose random country based on region
    data['COUNTRY'] = [np.random.choice(region_to_countries[region]) for region in
                       tqdm(data['REGION'], desc="Generating countries")]

    # Generate unique CRDS_CODE
    data['CRDS_CODE'] = [fake.random_number(digits=5, fix_len=True) for _ in
                         tqdm(range(num_rows), desc="Generating CRDS codes")]

    # Create random data for other columns
    for i in range(3, num_cols + 1):
        random_data_type = random.choice(data_types)
        data[f'col_{i}'] = [random_data_type() for _ in tqdm(range(num_rows), desc=f"Generating values for col_{i}")]

    df = pd.DataFrame(data)

    df.to_parquet('data_v2.parquet', engine='pyarrow', index=False)


# Function to shard the data
def shard_data(df, output_dir='shards'):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get the unique file identifiers (region, country, CRDS code)
    file_ids = df[['REGION', 'COUNTRY', 'CRDS_CODE']].drop_duplicates()

    # Create a shard for each file and save it
    for _, row in tqdm(file_ids.iterrows()):
        # Filter the DataFrame for the current identifiers
        df_file = df[(df['REGION'] == row['REGION']) &
                     (df['COUNTRY'] == row['COUNTRY']) &
                     (df['CRDS_CODE'] == row['CRDS_CODE'])]

        # Define the output file path
        file_path = os.path.join(output_dir, f"{row['REGION']}_{row['COUNTRY']}_{row['CRDS_CODE']}.parquet")

        # Save the current DataFrame shard to a parquet file
        df_file.to_parquet(file_path, engine='pyarrow', index=False)


def shard_data_v2(df, output_dir='shards'):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Sort and group the DataFrame by 'REGION', 'COUNTRY', and 'CRDS_CODE'
    df_sorted = df.sort_values(by=['REGION', 'COUNTRY', 'CRDS_CODE'])
    grouped = df_sorted.groupby(['REGION', 'COUNTRY', 'CRDS_CODE'])

    # Create a shard for each group and save it
    for name, group in tqdm(grouped, desc="Sharding data"):
        # Define the output file path
        file_path = os.path.join(output_dir, f"{name[0]}_{name[1]}_{name[2]}.parquet")

        # Save the current DataFrame shard to a parquet file
        group.to_parquet(file_path, engine='pyarrow', index=False)


def shard_data_v4(df, crds_per_group=1000, output_dir='shards_v4'):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Rank the CRDS codes within each country
    df = df.sort_values(by=['REGION', 'COUNTRY', 'CRDS_CODE'])
    df['CRDS_RANK'] = df.groupby(['REGION', 'COUNTRY']).cumcount()

    # Create the 'CRDS_GROUP' column
    df['CRDS_GROUP'] = np.floor(df['CRDS_RANK'] / crds_per_group).astype(int)

    # Drop the 'CRDS_RANK' column as it's no longer needed
    df = df.drop(columns=['CRDS_RANK'])

    # Group by 'REGION', 'COUNTRY', and 'CRDS_GROUP'
    grouped = df.groupby(['REGION', 'COUNTRY', 'CRDS_GROUP'])

    # Create a shard for each group and save it
    for name, group in tqdm(grouped, desc="Sharding data"):
        # Define the output file path
        file_path = os.path.join(output_dir, f"{name[0]}_{name[1]}_{name[2]}.parquet")

        # Save the current DataFrame shard to a parquet file
        group.to_parquet(file_path, engine='pyarrow', index=False)


def create_bloom_filters(df, output_dir='shards_v4'):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get the unique shard identifiers (region, country, CRDS group)
    shard_ids = df[['REGION', 'COUNTRY', 'CRDS_GROUP']].drop_duplicates()

    # Create a Bloom filter and associated filename for each shard and save it
    for _, row in shard_ids.iterrows():
        # Filter the DataFrame for the current identifiers
        df_shard = df[(df['REGION'] == row['REGION']) &
                      (df['COUNTRY'] == row['COUNTRY']) &
                      (df['CRDS_GROUP'] == row['CRDS_GROUP'])]

        # Create a Bloom filter
        bf = BloomFilter(capacity=len(df_shard), error_rate=0.1)  # Adjust the error rate as needed

        # Add the CRDS codes to the Bloom filter
        for crds_code in df_shard['CRDS_CODE']:
            bf.add(crds_code)

        # Define the shard file path
        shard_file_path = os.path.join(output_dir, f"{row['REGION']}_{row['COUNTRY']}_{row['CRDS_GROUP']}.parquet")

        # Define the output file path for the Bloom filter
        bloom_file_path = os.path.join(output_dir, f"{row['REGION']}_{row['COUNTRY']}_{row['CRDS_GROUP']}.bloom")

        # Save the Bloom filter to a file along with the shard file path
        bf.tofile(open(bloom_file_path, 'wb'))

        # Save the shard file path to a separate file
        with open(f"{bloom_file_path}_filepath.txt", 'w') as f:
            f.write(shard_file_path)


def search_data(crds_code, dir_path='shards_v4'):
    # Get the list of all Bloom filter files in the directory
    bloom_files = [f for f in os.listdir(dir_path) if f.endswith('.bloom')]

    # Check each Bloom filter
    for bloom_file in bloom_files:
        # Load the Bloom filter
        with open(os.path.join(dir_path, bloom_file), 'rb') as f:
            bf = BloomFilter.fromfile(f)

        # If the Bloom filter contains the CRDS code
        if crds_code in bf:
            # Load the shard file path
            with open(f"{os.path.join(dir_path, bloom_file)}_filepath.txt", 'r') as f:
                shard_file_path = f.read().strip()

            # Load the shard data
            df_shard = pd.read_parquet(shard_file_path)

            # Perform a precise check in the shard data
            if crds_code in df_shard['CRDS_CODE'].values:
                print(f"CRDS code {crds_code} found in shard {shard_file_path}")


# Main function
def main():
    # Generate the data
    # df = generate_data_v2()

    df = pd.read_parquet("data_v2.parquet")
    # Shard the data
    shard_data_v4(df)

    # Create the Bloom filters
    # create_bloom_filters(df)


# Call the main function
if __name__ == "__main__":
    main()
