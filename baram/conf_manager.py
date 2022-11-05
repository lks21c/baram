import os

import ujson as json

from baram.log_manager import LogManager


class ConfManager(object):

    @staticmethod
    def load_json_to_dict(filename: str) -> dict:
        """

        :param filename: json file name
        :return: dict
        """

        logger = LogManager.get_logger()

        conf_dir_path = os.path.join(os.getcwd(), '..', 'conf') \
            if 'tests' in os.getcwd() \
            else os.path.join(os.getcwd(), 'conf')
        try:
            with open(os.path.join(conf_dir_path, filename)) as f:
                return json.load(f)
        except Exception as e:
            logger.info(f'error {e}')
