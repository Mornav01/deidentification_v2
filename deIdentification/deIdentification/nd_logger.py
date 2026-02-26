import logging

# Create a logger
nd_logger = logging.getLogger(__name__)
nd_logger.setLevel(logging.DEBUG)  # Set the logging level

# Create a handler for terminal output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Set a formatter for the handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the handler to the logger
nd_logger.addHandler(console_handler)
