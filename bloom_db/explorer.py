import fnmatch
import os
import pickle
import threading

from pybloom_live import BloomFilter


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class TrieNode:
    def __init__(self, file_path=None):
        self.children = {}
        self.end_of_file = False
        self.bloom_filter = None
        self.file_path = file_path


class Trie(metaclass=Singleton):
    def __init__(self):
        self.root = TrieNode()

    def discover(self):
        self._dfs_discover(self.root, [])

    def _dfs_discover(self, node, path, depth=0):
        if node.end_of_file:
            print('  ' * depth + '/'.join(path))

        for part, child_node in node.children.items():
            print('  ' * depth + part)
            self._dfs_discover(child_node, path + [part], depth + 1)

    def size(self):
        return self._dfs_count(self.root)

    def _dfs_count(self, node):
        count = 1  # Counting this node
        for child in node.children.values():
            count += self._dfs_count(child)
        return count

    def insert(self, path, file_path=None):
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
        results = []
        self._dfs_search(self.root, path, 0, [], results)
        return results

    def _dfs_search(self, node, path, index, current_path, results):
        if index == len(path):
            if node.end_of_file:
                if node.bloom_filter is None:
                    node.bloom_filter = self.load_bloom_filter(node.file_path)
                    print(f'Loaded filter data from {node.file_path}: {node.bloom_filter}')
                results.append((current_path, node.bloom_filter))
            return

        part = path[index]

        for child_part, child_node in node.children.items():
            if fnmatch.fnmatch(child_part, part):
                self._dfs_search(child_node, path, index + 1, current_path + [child_part], results)

    @staticmethod
    def load_bloom_filter(path):
        try:
            with open(path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f'Error loading bloom filter from {path}: {e}')
            return None


def execute_query_v2(query, source, files, trie=None):
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
        matching_files = execute_query_v2(rules[0], source, files) if rules else set()
        for rule in rules[1:]:
            rule_files = execute_query_v2(rule, source, files)
            if condition == 'AND':
                matching_files.intersection_update(rule_files)
            elif condition == 'OR':
                matching_files.update(rule_files)
        return matching_files


def load_file_paths(root_directory, bloom_source):
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
    load_file_paths(r'C:\Users\medzi\Desktop\bnp\bloom\bloom', search_input["bloom_source"])
    # Execute the query
    import time
    x = time.time_ns()
    # Execute the query
    matching_files = execute_query_v2(search_input["query"], search_input["bloom_source"], search_input["files"])

    print(time.time_ns() - x)
    # Print matching files
    print(len(matching_files))
    for file in matching_files:
        print(f'Matching file: {file}')
