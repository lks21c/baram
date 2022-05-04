import pytest

from baram.log_manager import LogManager


@pytest.fixture()
def logger():
    return LogManager.get_logger()


def test_info_log(logger):
    logger.info('test')
