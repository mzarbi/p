import asyncio
import logging
import click
from petals.core import PetalsServer

logging.basicConfig(level=logging.INFO)


@click.command()
@click.option(
    "--host",
    default="127.0.0.1",
    help="The IP address the server should bind to. (default: 127.0.0.1)"
)
@click.option(
    "--port",
    default=8888,
    help="The port the server should listen on. (default: 8888)"
)
@click.option(
    "--bloom-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="The directory containing the bloom filters. This directory should contain the bloom filter files used for "
         "searching. "
)
def start_server(host, port, bloom_dir):
    """Start the Petals server and load bloom filters."""
    server = PetalsServer(host, port)
    server.load_bloom_filters(bloom_dir, "bloom")

    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        logging.info("Server stopping...")
    except Exception as e:
        logging.exception("Unexpected exception")
    finally:
        logging.info("Server shutdown.")


if __name__ == "__main__":
    start_server()