# DataBlooming - Efficient Data Indexing with Bloom Filters

## Introduction

DataBlooming is a Python class that provides an efficient way to index large datasets stored in Parquet files using Bloom Filters or range filters. The class is designed to help you quickly filter and access data without loading the entire dataset into memory. Additionally, DataBlooming supports reading from and writing to Amazon S3 for scalability and ease of use with cloud storage.

## Installation

To use DataBlooming, first, install the required dependencies by running the following command:

```bash
pip install pandas s3fs bloom-filter tqdm
```
## Usage
### Import DataBlooming:
```python
from datablooming import DataBlooming
```

### Create a DataBlooming instance:
```python
db = DataBlooming(
    error_rate=0.1,
    range_filter_threshold=1000,
    output_dir='bloom',
    read_from_s3=False,
    write_to_s3=False,
    s3_bucket=None
)
```
### Indexing Parquet files:
To index all Parquet files in a specific directory, use the index_files() method:

```python
directory = '/path/to/parquet_files'
db.index_files(directory)
```
### Indexing a single Parquet file:
You can also index a single Parquet file using the index_file() method:

```python
file_path = '/path/to/parquet_file.parquet'
bloom_path = '/path/to/output_directory'
db.index_file(file_path, bloom_path)
```

### Reading Indexed Data:
Once the data is indexed, you can retrieve the filters for specific columns using the filters attribute. For example:

```python
filter_data = db.filters['column_name']
```
### Usage with S3:
To use DataBlooming with Amazon S3, set the appropriate parameters when creating the instance:

```python
db = DataBlooming(
    error_rate=0.1,
    range_filter_threshold=1000,
    output_dir='s3://your-s3-bucket/bloom',
    read_from_s3=True,
    write_to_s3=True,
    s3_bucket='your-s3-bucket'
)
```
Make sure that you have the necessary permissions and credentials set up to read from and write to your S3 bucket.

Note: The above examples assume that the Parquet files are already available locally or on S3.

## DataBlooming Class Methods
The following methods are available in the DataBlooming class:

#### index_files(directory): 
Indexes all Parquet files in the specified directory using Bloom Filters or range filters.

#### index_file(file_path, bloom_path): 
Indexes a single Parquet file using Bloom Filters or range filters.

###Private Methods
#### _get_files(directory): 
Retrieves a list of Parquet file paths from the given directory.

#### _create_bloom_path(file_path): 
Creates the path where the indexed data filters will be stored based on the file_path.

#### _read_parquet(file_path): 
Reads the Parquet file located at file_path.

#### _write_pickle(data, file_path): 
Writes data to a Pickle file located at file_path.

### Important Notes
The error_rate parameter in the constructor defines the false positive rate for Bloom Filters. It must be a value between 0 and 1.

The range_filter_threshold parameter determines the number of unique values above which a column will be considered for a range filter instead of a Bloom Filter.

The output_dir parameter sets the directory where the indexed data filters will be stored. If using S3, you can specify a path like 's3://your-s3-bucket/bloom'.

The read_from_s3 and write_to_s3 parameters enable reading from and writing to Amazon S3, respectively. Make sure to provide the appropriate s3_bucket if using S3.