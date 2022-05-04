import pytest

from baram.kms_manager import KMSManager


@pytest.fixture()
def km():
    return KMSManager()


def test_list_keys(km):
    assert km.list_keys() is not None


def test_list_aliases(km):
    assert km.list_aliases() is not None


def test_arn(km):
    print(km.get_kms_arn('s3-hydra01-kms', False))
