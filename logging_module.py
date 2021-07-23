#!/usr/bin/python3

"""logging module
"""
import logging
from logging.handlers import RotatingFileHandler
import logging.handlers


def add_log(message):
    """

    :param message:
    """

    LOG_FILENAME = "logs.log"
    my_logger = logging.getLogger()
    my_logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=536870912, backupCount=1)

    my_logger.addHandler(handler)

    my_logger.info(message)
    my_logger.removeHandler(handler)







