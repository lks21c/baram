from logging import Logger
from typing import Optional

import requests
from requests import Response

from baram.log_manager import LogManager


class RequestsManager(object):

    @staticmethod
    def get(url: str,
            params: dict = None,
            cookies: dict = None,
            headers: dict = None) -> Optional[Response]:
        """

        :param url: url
        :param params: parameters
        :param cookies: cookies
        :param headers: headers
        :return:
        """
        logger: Logger = LogManager.get_logger()
        with requests.Session() as s:
            try:
                response: Response = s.get(url, params=params, cookies=cookies, headers=headers)
                return response
            except Exception as e:
                logger.info(e)
