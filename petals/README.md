# Petals Package

Petals is a Python package that provides functionality to interact with the Petals server.

## Installation

You can install the package using pip:

```bash
pip install petals
```

## Usage (Server)
To start the Petals server and load bloom filters, use the start-petals command with the following options:

--host: The IP address the server should bind to. (default: 127.0.0.1)
--port: The port the server should listen on. (default: 8888)
--bloom-dir: The directory containing the bloom filters. This directory should contain the bloom filter files used for searching.
Example:

```bash
start-petals --host 0.0.0.0 --port 8000 --bloom-dir /path/to/bloom/filters
```
Replace /path/to/bloom/filters with the actual directory containing the bloom filters.

The server will start listening on the specified IP address and port. Press Ctrl+C to stop the server.
## Usage (Client)
To use the PetalsClient to interact with the Petals server, follow these steps:

```python
from petals import PetalsClient
```

### Create a PetalsClient object
```python
client = PetalsClient('127.0.0.1', 8888)
```

### Send search query to the server
```python
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

matching_files = client.send_search_query(search_input)
for file_path in matching_files:
    print(file_path)
```