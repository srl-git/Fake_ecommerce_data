import logging
import google.cloud.logging

def setup_cloud_logging():

    client = google.cloud.logging.Client(_use_grpc=False)
    client.setup_logging()

def get_logger(name):

    logger = logging.getLogger(name)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    return logger


setup_cloud_logging()
