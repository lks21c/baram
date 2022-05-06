import sys
import logging


class LogManager(object):

    @staticmethod
    def get_logger(name=''):
        logger = logging.getLogger(name)
        logger.propagate = False
        logger.setLevel(logging.INFO)

        streamHandler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamHandler.setFormatter(formatter)
        logger.addHandler(streamHandler)

        return logger
