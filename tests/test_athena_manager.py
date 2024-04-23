import pytest

from baram.athena_manager import AthenaManager


@pytest.fixture()
def am():
    return AthenaManager()


