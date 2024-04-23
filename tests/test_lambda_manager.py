import pytest

from baram.lambda_manager import LambdaManager


@pytest.fixture()
def lm():
    return LambdaManager()


def test_list_layers(lm):
    layers = lm.list_layers()
    print(layers)
    assert len(layers) >= 0


# def test_get_latest_layer_arn(lm):
#     arn = lm.get_latest_layer_arn('ventus')
#     print(arn)
#     assert arn
#
#
# def test_publish_lambda_layer(lm):
#     sm = S3Manager('sli-dst-security')
#     lm.publish_lambda_layer('baram', 'sli-dst-security', sm)
