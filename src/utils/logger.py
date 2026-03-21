import logging
import sys


# factory function call to get back a logger named after that module it is called from
def get_logger(name: str) -> logging.Logger: 
    logger = logging.getLogger(name)

    if not logger.handlers: 
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %message)s",
            datefmt = "%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger

