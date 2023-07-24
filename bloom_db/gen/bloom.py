from pybloom_live import BloomFilter
import glob
import pandas as pd
import os
import numpy as np


class BloomSearchEngine:
    def __init__(self, error_rate=0.1, range_filter_threshold=1000):
        self.filters = {}
        self.error_rate = error_rate
        self.range_filter_threshold = range_filter_threshold

    def index_files(self, directory):
        # Find all parquet files in the directory
        files = glob.glob(os.path.join(directory, '*.parquet'))

        # Index each file
        for file_path in files:
            self.index_file(file_path)

    def index_file(self, file_path):
        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(file_path)

        # Create a filter for each column
        for column in df.columns:
            if df[column].dtype in (np.float, np.int, 'datetime64[ns]') and len(
                    df[column].unique()) > self.range_filter_threshold:
                # Use a range filter for float, integer and date columns with many unique values
                min_value = df[column].min()
                max_value = df[column].max()
                self.filters[(file_path, column)] = {
                    'type': 'range',
                    'min': min_value,
                    'max': max_value
                }
            else:
                # Use a Bloom filter otherwise
                bloom = BloomFilter(capacity=len(df[column]), error_rate=self.error_rate)
                for item in df[column]:
                    bloom.add(str(item))
                self.filters[(file_path, column)] = {
                    'type': 'bloom',
                    'filter': bloom
                }

    def execute_query(self, query):
        if 'column' in query and 'value' in query:
            # Base case: simple rule
            column = query['column']
            value = query['value']
            matching_files = set()
            for (file_path, column_name), filter_data in self.filters.items():
                if column_name == column:
                    if filter_data['type'] == 'bloom' and value in filter_data['filter']:
                        matching_files.add(file_path)
                    elif filter_data['type'] == 'range' and filter_data['min'] <= value <= filter_data['max']:
                        matching_files.add(file_path)
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
