import asyncio
import base64
import functools
import json
import logging
import os
import pickle
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

from explorer import Trie, execute_query_v2

logging.basicConfig(level=logging.INFO)


def ensure_json_output(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        if isinstance(result, str):
            result = {"response": result}  # Convert non-dict strings to JSON format
        return json.dumps(result)

    return wrapper


class TCPMessage:
    def __init__(self, cls, message_format, payload):
        self.cls = cls
        self.format = message_format
        self.payload = payload

    def to_xml(self):
        root = ET.Element(self.cls)
        root.set("format", self.format)
        root.text = escape(self.payload)  # ensure that payload is properly escaped
        return ET.tostring(root, encoding="unicode")


def parse_message(xml_string):
    root = ET.fromstring(xml_string)

    cls = root.tag
    format = root.get("format")
    payload = root.text.strip()

    # Parse the payload based on its format
    if format == "json":
        payload = json.loads(payload)
    elif format == "base64":
        payload = base64.b64decode(payload).decode('utf-8')

    return TCPMessage(cls, format, payload)


class BloomServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.handlers = {}
        self.sources = {}
        self.trie = Trie()

    def load_bloom_filters(self, root_directory, bloom_source):
        logging.info(f"Attempting to load bloom filters from {root_directory}")
        if not os.path.exists(root_directory):
            logging.error(f'Directory {root_directory} does not exist')
            return None

        for directory, subdirectories, files in os.walk(root_directory):
            for file in files:
                if not file.endswith('.pickle'):  # Assuming bloom filter files have a .pickle extension
                    continue
                full_path = os.path.join(directory, file)
                relative_path = [bloom_source] + os.path.relpath(full_path, root_directory).split(os.sep)
                self.trie.insert(relative_path, full_path)  # Use self.trie instead of trie

        return self.trie

    def message_handler(self, message_type):
        def decorator(func):
            self.handlers[message_type] = func
            return func

        return decorator

    async def handle_echo(self, reader, writer):
        buffer = ""
        while True:
            try:
                data = await asyncio.wait_for(reader.read(100), 10)
                buffer += data.decode()

                # Check if buffer ends with any of the registered message tags
                if any(buffer.endswith(f"</{message_type}>") for message_type in self.handlers):
                    break

            except asyncio.TimeoutError:
                logging.error("Connection timed out")
                writer.close()
                return

        try:
            message = parse_message(buffer)
        except ET.ParseError:
            logging.error('Invalid XML format')
            return

        addr = writer.get_extra_info('peername')

        if message.cls in self.handlers:
            response = await self.handlers[message.cls](message)
            writer.write(f"<{message.cls}>{response}</{message.cls}>".encode())
            logging.info(f"Processed {message.cls} from {addr!r}, sent: {response!r}")

        await writer.drain()
        logging.info("Closing the connection")
        writer.close()

    async def run(self):
        server = await asyncio.start_server(
            self.handle_echo, self.host, self.port)

        addr = server.sockets[0].getsockname()
        logging.info(f'Serving on {addr}')

        async with server:
            await server.serve_forever()


server = BloomServer('127.0.0.1', 8888)
server.load_bloom_filters(r'C:\Users\medzi\Desktop\bnp\bloom\bloom',
                          "bloom")  # Load bloom filters when server is created


@server.message_handler('search')
@ensure_json_output
async def search_handler(message: TCPMessage):
    # Parse the JSON payload into a dictionary
    search_input = message.payload

    # Validate the search_input dictionary
    if not isinstance(search_input,
                      dict) or "bloom_source" not in search_input or "files" not in search_input or "query" not in search_input:
        return {"error": "Invalid search_input"}

    # Extract search parameters
    bloom_source = search_input["bloom_source"]
    files = search_input["files"]
    query = search_input["query"]

    # Execute the query
    matching_files = execute_query_v2(query, bloom_source, files)

    # Convert the set of matching files to a list and return it
    return list(matching_files)


@server.message_handler('bloom')
@ensure_json_output
async def bloom_handler(message: TCPMessage):
    return 'alive'


@server.message_handler('ping')
@ensure_json_output
async def ping_handler(message: TCPMessage):
    print(server.sources)
    return 'alive'


@server.message_handler('message')
@ensure_json_output
async def message_handler(message: TCPMessage):
    return message.payload


if __name__ == "__main__":
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logging.info("Server stopping...")
    except Exception as e:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Server shutdown.")
