import glob
import os
import pickle

import pandas as pd
import numpy as np
from pybloom_live import BloomFilter
from tqdm import tqdm


class DataBlooming:
    def __init__(self, error_rate=0.1, range_filter_threshold=1000, output_dir='bloom'):
        self.filters = {}
        self.error_rate = error_rate
        self.range_filter_threshold = range_filter_threshold
        self.output_dir = output_dir

    def index_files(self, directory):
        # Find all parquet files in the directory
        files = glob.glob(os.path.join(directory, '*.parquet'))

        # Index each file
        for file_path in tqdm(files, desc="Processing files"):
            bloom_path = os.path.join(self.output_dir, os.path.basename(file_path).split(".")[0])
            os.makedirs(bloom_path, exist_ok=True)
            self.index_file(file_path, bloom_path)

    def load_blooms(self):
        self.filters = {}
        for tmp in tqdm(os.listdir(self.output_dir), "loading files"):
            self.filters.update({tmp: {}})
            for tmp2 in os.listdir(os.path.join(self.output_dir, tmp)):
                with open(os.path.join(self.output_dir, tmp, tmp2), "rb") as f:
                    filter = pickle.load(f)

                self.filters[tmp].update({
                    tmp2 : filter
                })

    def index_file(self, file_path, bloom_path):
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(file_path)

        # Create a filter for each column
        for column in df.columns:
            if df[column].dtype in (float, int, 'datetime64[ns]') and len(
                    df[column].unique()) > self.range_filter_threshold:
                # Use a range filter for float, integer and date columns with many unique values
                min_value = df[column].min()
                max_value = df[column].max()
                filter = {
                    'type': 'range',
                    'min': min_value,
                    'max': max_value
                }
            else:
                # Use a Bloom filter otherwise
                bloom = BloomFilter(capacity=len(df[column]), error_rate=self.error_rate)
                for item in df[column]:
                    bloom.add(str(item))
                filter =  {
                    'type': 'bloom',
                    'filter': bloom
                }


            with open(os.path.join(bloom_path, column.lower() + ".pickle"), "wb") as f:
                pickle.dump(filter, f)

    def execute_query(self, query):
        if 'column' in query and 'value' in query:
            # Base case: simple rule
            column = query['column']
            value = query['value']
            matching_files = set()
            if len(self.filters) == 0:
                raise Exception("No filters")

            for shard in self.filters:
                for column_name in self.filters[shard]:
                    if column_name.split(".pickle")[0] == column.lower():
                        filter_data = self.filters[shard][column_name]
                        if filter_data['type'] == 'bloom' and value in filter_data['filter']:
                            matching_files.add(shard)
                        elif filter_data['type'] == 'range' and filter_data['min'] <= value <= filter_data['max']:
                            matching_files.add(shard)
            return matching_files

        else:
            # Recursive case: AND/OR condition
            condition = query['condition']
            rules = query['rules']
            matching_files = self.execute_query(rules[0]) if rules else set()
            for rule in rules[1:]:
                rule_files = self.execute_query(rule)
                if condition == 'AND':
                    matching_files.intersection_update(rule_files)
                elif condition == 'OR':
                    matching_files.update(rule_files)
            return matching_files



engine = DataBlooming()
#db.index_files("shards")
engine.load_blooms()

matching_files = engine.execute_query({
    'condition': 'AND',
    'rules': [
        {
            'column': 'REGION',
            'value': 'EMEA'
        },
        {
            'column': 'Account_Type',
            'value': 'Mortgage'
        },
        {
            'column': 'Account_Balance',
            'value': '1500'
        }
    ]
})

# Print the files that might contain the required data
for file_path in matching_files:
    print(file_path)