import sys
import logging
from logging import Logger, Formatter, StreamHandler


class LogManager(object):

    @staticmethod
    def get_logger(name=''):
        logger: Logger = logging.getLogger(name)
        logger.propagate = False
        logger.setLevel(logging.INFO)

        streamHandler: StreamHandler = logging.StreamHandler(sys.stdout)
        formatter: Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

        return logger
