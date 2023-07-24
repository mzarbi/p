from bloom import BloomSearchEngine

engine = BloomSearchEngine()
engine.index_files(r'C:\Users\medzi\Desktop\bnp\bloom\shards_v4')

# Query the search engine with a complex query structure
matching_files = engine.execute_query({
    'condition': 'AND',
    'rules': [
        {
            'column': 'column1',
            'value': 'value1'
        },
        {
            'column': 'column2',
            'value': 'value2'
        },
        {
            'condition': 'OR',
            'rules': [
                {
                    'column': 'column3',
                    'value': 'value3'
                },
                {
                    'column': 'column4',
                    'value': 'value4'
                }
            ]
        }
    ]
})

# Print the files that might contain the required data
for file_path in matching_files:
    print(file_path)