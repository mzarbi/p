import fnmatch
import os
import glob
import pandas as pd
import s3fs
from tqdm import tqdm
from pybloom_live import BloomFilter
import pickle


class Singleton(type):
    """
    A metaclass that ensures only one instance of a class exists (Singleton pattern).

    This metaclass allows classes to have only one instance by keeping track of created instances
    and reusing them if they already exist.

    Example:
        class MyClass(metaclass=Singleton):
            pass

        obj1 = MyClass()
        obj2 = MyClass()

        # obj1 and obj2 will be the same instance.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Called when an instance of the class is created.

        If the class instance doesn't exist, create it and store it in the class's _instances dictionary.

        Args:
            cls: The class being instantiated.
            *args: Non-keyword arguments passed to the class constructor.
            **kwargs: Keyword arguments passed to the class constructor.

        Returns:
            object: The class instance.

        Example:
            See Singleton metaclass docstring for an example of usage.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class TrieNode:
    """
    Represents a node in a Trie data structure.

    TrieNode is used to build a Trie, where each node represents a part of a file path.

    Attributes:
        children (dict): A dictionary mapping child node parts to their respective TrieNode objects.
        end_of_file (bool): A flag indicating whether the node represents the end of a file path.
        bloom_filter (None or dict): A bloom filter used for fast approximate membership testing.
        file_path (None or str): The file path associated with the TrieNode, if it represents a file end.

    Example:
        node = TrieNode()
        node.children['folder'] = TrieNode()
        node.children['folder'].end_of_file = True
        node.children['folder'].file_path = 'path/to/folder'
    """

    def __init__(self, file_path=None):
        """
        Initialize a TrieNode object.

        Args:
            file_path (str or None): The file path associated with the TrieNode if it represents a file end,
                or None if it doesn't represent a file.
        """
        self.children = {}
        self.end_of_file = False
        self.bloom_filter = None
        self.file_path = file_path


class Trie(metaclass=Singleton):
    """
    Represents a Trie data structure with Singleton behavior.

    Trie is a tree-like data structure used for efficient string matching and searching.

    Example:
        trie = Trie()
        trie.insert(['path', 'to', 'file.txt'])
        trie.insert(['path', 'to', 'another', 'file.txt'])
        results = trie.search(['path', 'to'])
    """

    def __init__(self):
        """Initialize a Trie object with a root node."""
        self.root = TrieNode()

    def discover(self):
        """
        Discover and print all the file paths in the Trie.

        This method traverses the Trie and prints all the discovered file paths.

        Example:
            trie = Trie()
            trie.insert(['path', 'to', 'file.txt'])
            trie.insert(['path', 'to', 'another', 'file.txt'])
            trie.discover()
            # Output:
            # path
            #   to
            #     file.txt
            #     another
            #       file.txt
        """
        self._dfs_discover(self.root, [])

    def _dfs_discover(self, node, path, depth=0):
        """
        Depth-first search to discover and print file paths in the Trie.

        Args:
            node (TrieNode): The current node being traversed.
            path (list): The list of parts representing the current path.
            depth (int): The depth of the current node in the Trie.

        Example:
            See Trie.discover() for an example of usage.
        """
        if node.end_of_file:
            print('  ' * depth + '/'.join(path))

        for part, child_node in node.children.items():
            print('  ' * depth + part)
            self._dfs_discover(child_node, path + [part], depth + 1)

    def size(self):
        """
        Get the total number of nodes in the Trie.

        Returns:
            int: The number of nodes in the Trie.

        Example:
            trie = Trie()
            trie.insert(['path', 'to', 'file.txt'])
            trie.insert(['path', 'to', 'another', 'file.txt'])
            size = trie.size()
            # size will be 6 (including the root node).
        """
        return self._dfs_count(self.root)

    def _dfs_count(self, node):
        """
        Depth-first search to count the number of nodes in the Trie.

        Args:
            node (TrieNode): The current node being traversed.

        Returns:
            int: The number of nodes in the Trie starting from the given node.

        Example:
            See Trie.size() for an example of usage.
        """
        count = 1  # Counting this node
        for child in node.children.values():
            count += self._dfs_count(child)
        return count

    def insert(self, path, file_path=None):
        """
        Insert a file path into the Trie.

        Args:
            path (list): The list of parts representing the file path.
            file_path (str or None): The file path associated with the TrieNode if it represents a file end,
                or None if it doesn't represent a file.

        Example:
            trie = Trie()
            trie.insert(['path', 'to', 'file.txt'])
            trie.insert(['path', 'to', 'another', 'file.txt'])
        """
        node = self.root
        for part in path:
            if part not in node.children:
                node.children[part] = TrieNode(file_path)
            node = node.children[part]
        node.end_of_file = True
        if file_path:
            node.file_path = file_path
            node.bloom_filter = self.load_bloom_filter(file_path)

    def search(self, path):
        """
        Search the Trie for file paths matching the given path.

        Args:
            path (list): The list of parts representing the search path.

        Returns:
            list: A list of tuples containing the matching file paths and their associated bloom filters.

        Example:
            trie = Trie()
            trie.insert(['path', 'to', 'file.txt'], file_path='path/to/file.txt')
            trie.insert(['path', 'to', 'another', 'file.txt'], file_path='path/to/another/file.txt')
            results = trie.search(['path', 'to'])
            # results will be [('path/to/file.txt', None), ('path/to/another/file.txt', None)]
        """
        results = []
        self._dfs_search(self.root, path, 0, [], results)
        return results

    def _dfs_search(self, node, path, index, current_path, results):
        """
        Depth-first search to find file paths matching the given path.

        Args:
            node (TrieNode): The current node being traversed.
            path (list): The list of parts representing the search path.
            index (int): The current index in the search path.
            current_path (list): The list of parts representing the current path.
            results (list): A list to store the matching file paths and their bloom filters.

        Example:
            See Trie.search() for an example of usage.
        """
        if index == len(path):
            if node.end_of_file:
                if node.bloom_filter is None:
                    node.bloom_filter = self.load_bloom_filter(node.file_path)
                    print(f'Loaded filter data from {node.file_path}: {node.bloom_filter}')
                results.append((os.path.dirname('/'.join(current_path)), node.bloom_filter))
            return

        part = path[index]

        for child_part, child_node in node.children.items():
            if fnmatch.fnmatch(child_part, part):
                self._dfs_search(child_node, path, index + 1, current_path + [child_part], results)

    @staticmethod
    def load_bloom_filter(path):
        """
        Load a bloom filter from a file.

        Args:
            path (str): The file path to the pickle file containing the bloom filter.

        Returns:
            None or dict: The loaded bloom filter if successful, or None if an error occurred.

        Example:
            filter_data = Trie.load_bloom_filter('path/to/bloom_filter.pickle')
        """
        try:
            with open(path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f'Error loading bloom filter from {path}: {e}')
            return None


def execute_query_v2(query, source, files, trie=None):
    """
    Execute a query on the Trie to find matching files based on specified conditions.

    Args:
        query (dict): The query dictionary containing conditions and rules for the search.
        source (str): The source of the files to be searched (e.g., 'bloom', 'APAC').
        files (str): The files pattern to be matched (e.g., 'APAC_AUS_*').
        trie (Trie or None): The Trie object to use for searching. If None, a new Trie will be created.

    Returns:
        set: A set of matching file paths.

    Example:
        search_input = {
            "bloom_source": "bloom",
            "files": "APAC_AUS_*",
            "query": {
                'condition': 'AND',
                'rules': [
                    {
                        'column': 'account_status',
                        'value': 'Inactive'
                    },
                    {
                        'column': 'account_type',
                        'value': 'Savings'
                    },
                    {
                        'column': 'loan_status',
                        'value': 'Current'
                    }
                ]
            }
        }
        matching_files = execute_query_v2(search_input["query"], search_input["bloom_source"], search_input["files"])
    """
    if not trie:
        trie = Trie()

    if 'column' in query and 'value' in query:
        # Base case: simple rule
        column = query['column']
        value = query['value']

        bloom_filters = trie.search([source, files, column + "*.pickle"])
        matching_files = set()
        for path, filter_data in bloom_filters:
            if filter_data:
                if filter_data['type'] == 'bloom' and value in filter_data['filter']:
                    matching_files.add(os.path.dirname('/'.join(path)))
                elif filter_data['type'] == 'range' and filter_data['min'] <= value <= filter_data['max']:
                    matching_files.add(os.path.dirname('/'.join(path)))
        return matching_files
    else:
        # Recursive case: AND/OR condition
        condition = query['condition']
        rules = query['rules']
        matching_files = execute_query_v2(rules[0], source, files, trie) if rules else set()
        for rule in rules[1:]:
            rule_files = execute_query_v2(rule, source, files, trie)
            if condition == 'AND':
                matching_files.intersection_update(rule_files)
            elif condition == 'OR':
                matching_files.update(rule_files)
        return matching_files


def load_file_paths(root_directory, bloom_source):
    """
    Load file paths into a Trie from the given root directory.

    Args:
        root_directory (str): The root directory from which to load file paths.
        bloom_source (str): The source identifier for the files (e.g., 'bloom').

    Returns:
        Trie or None: The Trie object containing the loaded file paths, or None if the root directory doesn't exist.

    Example:
        trie = load_file_paths(r'D:\\apps\\bloom\\bloom', 'bloom')
    """
    if not os.path.exists(root_directory):
        print(f'Directory {root_directory} does not exist')
        return None

    trie = Trie()

    for directory, subdirectories, files in os.walk(root_directory):
        for file in files:
            if not file.endswith('.pickle'):  # Assuming bloom filter files have a .pickle extension
                continue
            full_path = os.path.join(directory, file)
            relative_path = [bloom_source] + os.path.relpath(full_path, root_directory).split(os.sep)
            trie.insert(relative_path, full_path)

    return trie


class DataBlooming:
    """
    DataBlooming is a class that performs data indexing using Bloom Filters or range filters on large datasets
    stored in Parquet files. It allows you to efficiently index and filter data in-memory and can optionally
    read and write data from/to Amazon S3.

    Parameters:
    - error_rate (float): The false positive rate for the Bloom Filters. Must be a value between 0 and 1.
    - range_filter_threshold (int): The threshold for considering a column as a range filter candidate.
      If the number of unique values in a column exceeds this threshold, a range filter will be used.
    - output_dir (str): The output directory where the indexed data will be stored.
    - read_from_s3 (bool): If True, the class will read data from Amazon S3.
    - write_to_s3 (bool): If True, the class will write indexed data to Amazon S3.
    - s3_bucket (str): The name of the S3 bucket used for reading and writing data (optional).

    Attributes:
    - filters (dict): A dictionary to store the indexed data filters.
    - error_rate (float): The false positive rate for the Bloom Filters.
    - range_filter_threshold (int): The threshold for considering a column as a range filter candidate.
    - output_dir (str): The output directory where the indexed data will be stored.
    - read_from_s3 (bool): If True, the class will read data from Amazon S3.
    - write_to_s3 (bool): If True, the class will write indexed data to Amazon S3.
    - fs (s3fs.S3FileSystem): The S3 file system object used for S3 operations (if read_from_s3 or write_to_s3 is True).
    - s3_bucket (str): The name of the S3 bucket used for reading and writing data (optional).

    Methods:
    - index_files(directory):
        Indexes all the Parquet files present in the given directory using Bloom Filters or range filters.

        Parameters:
        - directory (str): The directory containing the Parquet files to be indexed.

    - index_file(file_path, bloom_path):
        Indexes a single Parquet file using Bloom Filters or range filters.

        Parameters:
        - file_path (str): The path to the Parquet file to be indexed.
        - bloom_path (str): The path where the indexed data filters will be stored.

    Private Methods:
    - _get_files(directory):
        Retrieves a list of Parquet file paths from the given directory.

        Parameters:
        - directory (str): The directory containing the Parquet files.

        Returns:
        - files (list): A list of file paths for the Parquet files in the directory.

    - _create_bloom_path(file_path):
        Creates the path where the indexed data filters will be stored based on the file_path.

        Parameters:
        - file_path (str): The path to the Parquet file being indexed.

        Returns:
        - bloom_path (str): The path where the indexed data filters will be stored.

    - _read_parquet(file_path):
        Reads the Parquet file located at file_path.

        Parameters:
        - file_path (str): The path to the Parquet file.

        Returns:
        - df (pandas.DataFrame): The DataFrame containing the data read from the Parquet file.

    - _write_pickle(data, file_path):
        Writes data to a Pickle file located at file_path.

        Parameters:
        - data (any): The data to be written to the Pickle file.
        - file_path (str): The path to the Pickle file.
    """
    def __init__(self, error_rate=0.1, range_filter_threshold=1000, output_dir='bloom',
                 read_from_s3=False, write_to_s3=False, s3_bucket=None):
        self.filters = {}
        self.error_rate = error_rate
        self.range_filter_threshold = range_filter_threshold
        self.output_dir = output_dir
        self.read_from_s3 = read_from_s3
        self.write_to_s3 = write_to_s3
        self.fs = s3fs.S3FileSystem() if read_from_s3 or write_to_s3 else None
        self.s3_bucket = s3_bucket if read_from_s3 or write_to_s3 else None

    def index_files(self, directory):
        """
        Indexes all the Parquet files present in the given directory using Bloom Filters
        or range filters. The indexed data filters are stored in separate files in the
        specified output directory.

        Parameters:
        - directory (str): The directory containing the Parquet files to be indexed.

        Returns:
        - None
        """
        files = self._get_files(directory)

        for file_path in tqdm(files, desc="Processing files"):
            bloom_path = self._create_bloom_path(file_path)
            self.index_file(file_path, bloom_path)

    def index_file(self, file_path, bloom_path):
        """
        Indexes a single Parquet file using Bloom Filters or range filters. The indexed
        data filter for each column is stored as a separate file in the specified bloom_path.

        Parameters:
        - file_path (str): The path to the Parquet file to be indexed.
        - bloom_path (str): The path where the indexed data filters will be stored.

        Returns:
        - None
        """
        df = self._read_parquet(file_path)

        for column in df.columns:
            if df[column].dtype in (float, int, 'datetime64[ns]') and len(
                    df[column].unique()) > self.range_filter_threshold:
                min_value = df[column].min()
                max_value = df[column].max()
                filter_data = {
                    'type': 'range',
                    'min': min_value,
                    'max': max_value
                }
            else:
                bloom = BloomFilter(capacity=len(df[column]), error_rate=self.error_rate)
                for item in df[column]:
                    bloom.add(str(item))
                filter_data = {
                    'type': 'bloom',
                    'filter': bloom
                }

            filter_file_path = os.path.join(bloom_path, column.lower() + ".pickle")
            self._write_pickle(filter_data, filter_file_path)

    def _get_files(self, directory):
        """
        Retrieves a list of Parquet file paths from the given directory.

        Parameters:
        - directory (str): The directory containing the Parquet files.

        Returns:
        - files (list): A list of file paths for the Parquet files in the directory.
        """
        if self.read_from_s3:
            full_path = os.path.join(self.s3_bucket, directory)
            return self.fs.glob(full_path + '/*.parquet')
        else:
            return glob.glob(os.path.join(directory, '*.parquet'))

    def _create_bloom_path(self, file_path):
        """
        Creates the path where the indexed data filters will be stored based on the file_path.
        If write_to_s3 is True, the path is created inside the specified S3 bucket.

        Parameters:
        - file_path (str): The path to the Parquet file being indexed.

        Returns:
        - bloom_path (str): The path where the indexed data filters will be stored.
        """
        if self.write_to_s3:
            bloom_path = os.path.join(self.s3_bucket, self.output_dir, os.path.basename(file_path).split(".")[0])
        else:
            bloom_path = os.path.join(self.output_dir, os.path.basename(file_path).split(".")[0])
            os.makedirs(bloom_path, exist_ok=True)
        return bloom_path

    def _read_parquet(self, file_path):
        """
        Reads the Parquet file located at file_path. If read_from_s3 is True, the file is read
        from the S3 bucket specified in s3_bucket.

        Parameters:
        - file_path (str): The path to the Parquet file.

        Returns:
        - df (pandas.DataFrame): The DataFrame containing the data read from the Parquet file.
        """
        if self.read_from_s3:
            return pd.read_parquet(self.fs.open(file_path))
        else:
            return pd.read_parquet(file_path)

    def _write_pickle(self, data, file_path):
        """
        Writes data to a Pickle file located at file_path. If write_to_s3 is True, the file is
        written to the S3 bucket specified in s3_bucket.

        Parameters:
        - data (any): The data to be written to the Pickle file.
        - file_path (str): The path to the Pickle file.

        Returns:
        - None
        """
        if self.write_to_s3:
            with self.fs.open(file_path, "wb") as f:
                pickle.dump(data, f)
        else:
            with open(file_path, "wb") as f:
                pickle.dump(data, f)


if __name__ == "__main__":
    # Define the search input
    search_input = {
        "bloom_source": "bloom",
        "files": "APAC_AUS_*",
        "query": {
            'condition': 'AND',
            'rules': [
                {
                    'column': 'account_status',
                    'value': 'Inactive'
                },
                {
                    'column': 'account_type',
                    'value': 'Savings'
                },
                {
                    'column': 'loan_status',
                    'value': 'Current'
                }
            ]
        }
    }

    # Load all bloom filters into Trie
    load_file_paths(r'D:\\apps\\bloom\\bloom', search_input["bloom_source"])

    # Execute the query
    import time

    x = time.time_ns()
    matching_files = execute_query_v2(search_input["query"], search_input["bloom_source"], search_input["files"])
    print(time.time_ns() - x)

    # Print matching files
    print(len(matching_files))
    for file in matching_files:
        print(f'Matching file: {file}')
