import logging
from logging import Logger

import google.cloud.logging
from dotenv import load_dotenv

from shared.config import get_config

load_dotenv()
config = get_config()


def setup_logging() -> None:
    """
    Configure logging based on the environment configuration.

    Initializes a Google Cloud Logging client and attaches a handler to the root python logger if cloud logging is enabled
    else configures standard Python logging to output to the console with a formatted message.

    """
    if config.USE_CLOUD_LOGGING:
        client = google.cloud.logging.Client(_use_grpc=False)
        client.setup_logging(log_level=getattr(logging, config.LOG_LEVEL))
    else:
        logging.basicConfig(
            level=getattr(logging, config.LOG_LEVEL),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )
    logging.getLogger("faker").setLevel(logging.WARNING)


def get_logger(name: str) -> Logger:
    """
    Creates and returns a named logger with configured log level.

    Args:
        name (str): The name of the logger.

    Returns:
        Logger: A configured logger instance.

    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    return logger
