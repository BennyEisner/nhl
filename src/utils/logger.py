import logging
import sys


# factory function call to get back a logger named after that module it is called from
def get_logger(name: str) -> logging.Logger: 
    logger = logging.getLogger(name)

    if not logger.handlers: 
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="{asctime} | {levelname:<8} | {name} | {message}",
            datefmt="%Y-%m-%d %H:%M:%S",
            style="{"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger

