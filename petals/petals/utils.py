import base64
import functools
import json
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape


def ensure_json_output(func):
    """
    A decorator function to ensure that the output of the decorated function is in JSON format.

    Args:
        func (coroutine function): The asynchronous function to be decorated.

    Returns:
        coroutine function: A wrapper function that converts the result of the original function
        to JSON format if it is a string.

    Example:
        @ensure_json_output
        async def get_data():
            return "Hello, World!"

        # The output of get_data() will be '{"response": "Hello, World!"}'
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        if isinstance(result, str):
            result = {"response": result}  # Convert non-dict strings to JSON format
        return json.dumps(result)

    return wrapper


class TCPMessage:
    """
    Represents a TCP message with its class, format, and payload.

    Attributes:
        cls (str): The class of the message.
        format (str): The format of the message payload.
        payload (str): The payload of the message.

    Methods:
        to_xml(): Converts the TCPMessage object to an XML string.

    Example:
        msg = TCPMessage("Notification", "json", '{"message": "Hello, World!"}')
        xml_string = msg.to_xml()
        # Resulting XML string: '<Notification format="json">{"message": "Hello, World!"}</Notification>'
    """

    def __init__(self, cls, message_format, payload):
        """
        Initialize a TCPMessage object.

        Args:
            cls (str): The class of the message.
            message_format (str): The format of the message payload (e.g., 'json', 'base64', etc.).
            payload (str): The payload of the message.
        """
        self.cls = cls
        self.format = message_format
        self.payload = payload

    def to_xml(self):
        """
        Converts the TCPMessage object to an XML string.

        Returns:
            str: The XML representation of the TCPMessage object.

        Example:
            msg = TCPMessage("Notification", "json", '{"message": "Hello, World!"}')
            xml_string = msg.to_xml()
            # Resulting XML string: '<Notification format="json">{"message": "Hello, World!"}</Notification>'
        """
        root = ET.Element(self.cls)
        root.set("format", self.format)
        root.text = escape(self.payload)  # ensure that payload is properly escaped
        return ET.tostring(root, encoding="unicode")


def parse_message(xml_string):
    """
    Parses an XML string representing a TCP message and creates a TCPMessage object.

    Args:
        xml_string (str): The XML string representing the TCP message.

    Returns:
        TCPMessage: A TCPMessage object containing the class, format, and payload of the message.

    Example:
        xml_string = '<Notification format="json">{"message": "Hello, World!"}</Notification>'
        message = parse_message(xml_string)
        # Returns a TCPMessage object with cls="Notification", format="json", and payload='{"message": "Hello, World!"}'
    """
    root = ET.fromstring(xml_string)

    cls = root.tag
    message_format = root.get("format")
    payload = root.text.strip()

    # Parse the payload based on its format
    if message_format == "json":
        payload = json.loads(payload)
    elif message_format == "base64":
        payload = base64.b64decode(payload).decode('utf-8')

    return TCPMessage(cls, message_format, payload)
