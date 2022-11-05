import requests

from baram.log_manager import LogManager


class RequestsManager(object):

    def __init__(self):
        self.logger = LogManager.get_logger()

    def get(self,
            url: str,
            params: dict = None,
            cookies: dict = None,
            headers: dict = None) -> dict:
        """

        :param url: url
        :param params: parameters
        :param cookies: cookies
        :param headers: headers
        :return:
        """
        with requests.Session() as s:
            try:
                response = s.get(url, params=params, cookies=cookies, headers=headers)
                return response
            except Exception as e:
                self.logger.info(e)
