from pprint import pprint

import pytest

from baram.iam_manager import IAMManager


@pytest.fixture()
def im():
    return IAMManager()


def test_get_role(im):
    pprint(im.get_role('AFlow-developer-role'))
    assert im.get_role('AFlow-developer-role')


def test_list_role_policies(im):
    pprint(im.list_role_policies('AFlow-developer-role'))
    assert im.list_role_policies('AFlow-developer-role')


def test_list_group_policies(im):
    pprint(im.list_group_policies('sagemaker_studio_user'))
    assert im.list_group_policies('sagemaker_studio_user')


def test_role_arn(im):
    pprint(im.get_role_arn('AFlow-developer-role'))
    assert im.get_role_arn('AFlow-developer-role')
