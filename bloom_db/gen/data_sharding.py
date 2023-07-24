import os
import pandas as pd
import numpy as np
from tqdm import tqdm


def shard_data(df, crds_per_group=10000, output_dir='shards_5M'):
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


df = pd.read_parquet("data_v3.parquet")
# Shard the data
shard_data(df)