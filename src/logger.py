import os
import sys

import __main__
import logging
from logging.handlers import RotatingFileHandler

_logger_singleton = None

def get_logger(log_to_file=True):
    global _logger_singleton

    if _logger_singleton:
        return _logger_singleton

    logger = logging.getLogger()
    logger.setLevel(os.environ.get('LOG_LEVEL', logging.DEBUG))
    app_name = os.path.basename(__main__.__file__).strip(".py")
    formatter = logging.Formatter(f"[%(asctime)s] %(levelname)s [{app_name}][%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                                  datefmt='%Y-%m-%dT%H:%M:%S')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)

    if log_to_file:
        path = f"/var/log/{app_name}"
        if not os.path.exists(path):
            os.makedirs(path)
    
        file_handler = RotatingFileHandler(path + "/application.log", maxBytes=100000, backupCount=10)
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)

    _logger_singleton = logger

    return logger
