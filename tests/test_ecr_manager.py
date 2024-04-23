import pytest

from baram.ecr_manager import ECRManager


@pytest.fixture()
def ecr():
    return ECRManager()


# def test_describe_image(ecr):
#     # TODO: add sample image and check it
#     images = ecr.describe_images('sli-docker')
#     assert images
#     pprint(images)
#
#
# def test_list_images(ecr):
#     # TODO: add sample image and check it
#     images = ecr.list_images('sli-docker')
#     assert images
#     pprint(images)