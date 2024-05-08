import ujson as json

from baram.log_manager import LogManager


class JsonManager(object):
    def __init__(self):
        self.logger = LogManager.get_logger()

    @staticmethod
    def load_json(path: str):
        with open(path) as f:
            return json.load(f)
