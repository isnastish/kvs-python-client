import logging

from .cli import hello

_logger = logging.getLogger(__name__)

if __name__ == '__main__':
    _logger.info("Invoked main procedure")
    hello()