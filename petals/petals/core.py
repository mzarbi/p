import asyncio
import logging
import os
import xml.etree.ElementTree as ET
from petals.bloom import Trie
from petals.utils import parse_message


class PetalsServer:
    """
    Represents a server for handling and processing messages in the Petals system.

    The PetalsServer listens for incoming connections, processes XML messages,
    and dispatches the appropriate message handler based on the message type.

    Attributes:
        host (str): The host address on which the server listens for connections.
        port (int): The port number on which the server listens for connections.
        handlers (dict): A dictionary mapping message types to their corresponding message handlers.
        sources (dict): A dictionary to store sources of bloom filters.
        trie (Trie): A Trie object to store and search for bloom filter file paths.

    Methods:
        load_bloom_filters(root_directory, bloom_source):
            Load bloom filters from the given root directory into the Trie.

        message_handler(message_type):
            A decorator function to register message handlers for specific message types.

        handle_echo(reader, writer):
            The coroutine to handle an incoming connection and process messages.

        run():
            Start the PetalsServer and run the event loop to handle incoming connections.

    Example:
        # Initialize and run the PetalsServer
        server = PetalsServer('127.0.0.1', 8080)
        asyncio.run(server.run())
    """

    def __init__(self, host, port):
        """
        Initialize a PetalsServer object with the specified host and port.

        Args:
            host (str): The host address on which the server listens for connections.
            port (int): The port number on which the server listens for connections.
        """
        self.host = host
        self.port = port
        self.handlers = {}
        self.sources = {}
        self.trie = Trie()

    def load_bloom_filters(self, root_directory, bloom_source):
        """
        Load bloom filters from the given root directory into the Trie.

        This method traverses the root_directory, identifies bloom filter files with the '.pickle' extension,
        and inserts their relative file paths into the Trie for efficient searching.

        Args:
            root_directory (str): The root directory from which to load bloom filter files.
            bloom_source (str): The source identifier for the bloom filters.

        Returns:
            Trie or None: The Trie object containing the loaded file paths, or None if the root directory
            doesn't exist or if an error occurred during loading.

        Example:
            server = PetalsServer('127.0.0.1', 8080)
            trie = server.load_bloom_filters('/path/to/bloom', 'bloom_source')
        """
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
                self.trie.insert(relative_path, full_path)

        return self.trie

    def message_handler(self, message_type):
        """
        A decorator function to register message handlers for specific message types.

        This decorator allows methods to be used as message handlers for the specified message type.

        Args:
            message_type (str): The message type associated with the decorated method.

        Returns:
            function: The original method decorated as a message handler.

        Example:
            server = PetalsServer('127.0.0.1', 8080)

            @server.message_handler('Notification')
            async def handle_notification(message):
                # Handle the Notification message
                return "Notification received"

            # The 'handle_notification' method will be called when a 'Notification' message is received.
        """
        def decorator(func):
            self.handlers[message_type] = func
            return func

        return decorator

    async def handle_echo(self, reader, writer):
        """
        The coroutine to handle an incoming connection and process messages.

        This coroutine reads data from the connection's reader and accumulates it in a buffer.
        When the buffer ends with any of the registered message tags, it parses the XML message,
        extracts its class, and dispatches the appropriate message handler based on the class.

        Args:
            reader (asyncio.StreamReader): The connection's reader to read data from the client.
            writer (asyncio.StreamWriter): The connection's writer to send data to the client.

        Example:
            See the 'run' method for an example of usage.
        """
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
        """
        Start the PetalsServer and run the event loop to handle incoming connections.

        This coroutine creates a server to listen for incoming connections on the specified host and port.
        It processes incoming connections by calling the 'handle_echo' coroutine.

        Example:
            server = PetalsServer('127.0.0.1', 8080)
            asyncio.run(server.run())
        """
        server = await asyncio.start_server(
            self.handle_echo, self.host, self.port)

        addr = server.sockets[0].getsockname()
        logging.info(f'Serving on {addr}')

        async with server:
            await server.serve_forever()
