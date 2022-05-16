import pytest
from pprint import pprint

from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def sm():
    return SagemakerManager()


def test_describe_image(sm):
    response = sm.describe_image('sli-docker')
    assert response
    pprint(response)

def test_describe_image_version(sm):
    response = sm.describe_image_version('sli-docker')
    assert response['Version']
    pprint(response['Version'])

