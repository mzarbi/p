import socket
import json
import asyncio
import xml.etree.ElementTree as ET
import base64
import pickle

from server import TCPMessage


class Client:
    def __init__(self, host, port, retries=3, timeout=10):
        self.host = host
        self.port = port
        self.retries = retries
        self.timeout = timeout

    async def send_search_query(self, search_input):
        # Convert the search_input dictionary to a JSON string
        search_input_json = json.dumps(search_input)

        # Create a message in XML format
        message = TCPMessage("search", "json", search_input_json)

        # Send the message to the server and receive the response
        response = await self.send_message(message)

        # response is a Python object (a list of file paths) decoded by parse_response
        matching_files = response

        return matching_files

    async def send_message(self, message):
        xml_string = message.to_xml()
        for attempt in range(self.retries):
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout)

                writer.write(xml_string.encode())
                await writer.drain()

                buffer = ""
                while True:
                    chunk = await asyncio.wait_for(
                        reader.read(1024),
                        timeout=self.timeout)

                    if not chunk:
                        break
                    buffer += chunk.decode()
                    if buffer.endswith(f"</{message.cls}>"):
                        break

                response_string = buffer.replace(f"<{message.cls}>", "").replace(f"</{message.cls}>", "").strip()
                response = self.parse_response(message.format, response_string)

                print(f"Received from server: {response}")

                writer.close()
                await writer.wait_closed()

                return response

            except (asyncio.TimeoutError, ConnectionRefusedError) as e:
                print(f"Attempt {attempt + 1} failed. Retrying.")
                await asyncio.sleep(2 ** attempt)

        raise Exception("Server is not responding.")

    def parse_response(self, format, response_string):
        if format == "text":
            return response_string
        elif format == "json":
            return json.loads(response_string)
        elif format == "base64":
            return base64.b64decode(response_string).decode()
        else:
            raise ValueError(f"Unexpected response format: {format}")


# Example usage:
if __name__ == "__main__":
    client = Client('127.0.0.1', 8888)

    # Creating a message with text format
    message = TCPMessage("message", "text", "Hello server!")
    asyncio.run(client.send_message(message))

    # Creating a message with JSON format
    message = TCPMessage("message", "json", json.dumps({"key": "value"}))
    asyncio.run(client.send_message(message))

    # Creating a message with pickle format
    message = TCPMessage("message", "text", base64.b64encode(pickle.dumps({"key": "value"})).decode())
    asyncio.run(client.send_message(message))

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

    # Creating a search message in JSON format
    search_message = TCPMessage("search", "json", json.dumps(search_input))
    matching_files = asyncio.run(client.send_message(search_message))

    # Print the list of matching files
    for file_path in matching_files:
        print(file_path)