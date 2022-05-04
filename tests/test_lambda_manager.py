import pytest

from baram.kms_manager import KMSManager
from baram.s3_manager import S3Manager
from baram.lambda_manager import LambdaManager


@pytest.fixture()
def lm():
    return LambdaManager()


def test_list_layers(lm):
    print(lm.list_layers())
    assert lm.list_layers() is not None


def test_get_latest_layer_arn(lm):
    print(lm.get_latest_layer_arn('ventus'))
    assert lm.get_latest_layer_arn('ventus') is not None


def test_publish_lambda_layer(lm):
    sm = S3Manager('sli-dst-security', KMSManager().get_kms_arn('s3-hydra01-kms', False))
    lm.publish_lambda_layer('baram', 'sli-dst-security', sm)
