import pytest

from baram.kms_manager import KMSManager


@pytest.fixture()
def km():
    return KMSManager()


def test_list_keys(km):
    assert len(km.list_keys()) >= 0


def test_list_aliases(km):
    assert len(km.list_aliases()) >= 0