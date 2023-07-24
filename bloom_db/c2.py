import asyncio
import json
import xml.etree.ElementTree as ET


def create_message(cls, format, payload):
    root = ET.Element(cls)
    root.set("format", format)
    root.text = payload
    return ET.tostring(root, encoding="unicode")


class BloomClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    async def send_search_query(self, search_input):
        # Convert the search_input dictionary to a JSON string
        search_input_json = json.dumps(search_input)

        # Create a message in XML format
        message = create_message("search", "json", search_input_json)

        # Send the message to the server and receive the response
        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(message.encode())
        await writer.drain()

        response = await reader.read(10000)
        writer.close()

        # The server responds with a string in the format "<search>{response}</search>"
        # We need to extract the response from between the "<search>" and "</search>" tags
        start_index = response.find("<search>") + len("<search>")
        end_index = response.find("</search>")
        response_json = response[start_index:end_index]

        # Convert the response from JSON format to a Python object (a list of file paths)
        matching_files = json.loads(response_json)

        return matching_files


if __name__ == "__main__":
    client = BloomClient('127.0.0.1', 8888)

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

    # Send the search input to the server
    matching_files = asyncio.run(client.send_search_query(search_input))

    # Print the list of matching files
    for file_path in matching_files:
        print(file_path)