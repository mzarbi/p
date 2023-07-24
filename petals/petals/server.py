import asyncio
import logging

from petals.bloom import execute_query_v2
from petals.core import PetalsServer
from petals.utils import ensure_json_output, TCPMessage

logging.basicConfig(level=logging.INFO)

server = PetalsServer('127.0.0.1', 8888)
server.load_bloom_filters(r'C:\Users\medzi\Desktop\bnp\bloom\bloom', "bloom")


# Load bloom filters when the server is created.

@server.message_handler('search')
@ensure_json_output
async def search_handler(message: TCPMessage):
    """
    Message handler to process a 'search' request.

    This handler is decorated to handle messages of type 'search' received by the PetalsServer.
    It expects the message payload to be a JSON-formatted search input containing 'bloom_source',
    'files', and 'query' keys.

    Args:
        message (TCPMessage): The TCPMessage object representing the received message.

    Returns:
        dict: A JSON-formatted dictionary containing the list of matching files or an error message.

    Example:
        # Send a 'search' request to the server
        request_message = TCPMessage('search', 'json', '{"bloom_source": "bloom", "files": "APAC_AUS_*", "query": { ... }}')
        response = search_handler(request_message)
    """
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


@server.message_handler('ping')
@ensure_json_output
async def ping_handler(message: TCPMessage):
    """
    Message handler to process a 'ping' request.

    This handler is decorated to handle messages of type 'ping' received by the PetalsServer.
    It simply returns a response of 'alive' in JSON format to indicate that the server is running.

    Args:
        message (TCPMessage): The TCPMessage object representing the received message.

    Returns:
        dict: A JSON-formatted dictionary containing the response 'alive'.

    Example:
        # Send a 'ping' request to the server
        request_message = TCPMessage('ping', 'json', '')
        response = ping_handler(request_message)
    """
    return 'alive'


if __name__ == "__main__":
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logging.info("Server stopping...")
    except Exception as e:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Server shutdown.")
