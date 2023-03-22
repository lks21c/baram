import pytest
from pprint import pprint
from baram.iam_manager import IAMManager


@pytest.fixture()
def im():
    return IAMManager()


def test_get_role(im):
    # Given
    role_name = 'AWSServiceRoleForLakeFormationDataAccess'

    # When
    role = im.get_role(role_name)

    # Then
    assert role is not None


def test_list_role_policies(im):
    # Given
    role_name = 'AWSServiceRoleForLakeFormationDataAccess'

    # When
    policies = im.list_role_policies(role_name)

    # Then
    assert policies is not None


def test_role_arn(im):
    # Given
    role_name = 'AWSServiceRoleForLakeFormationDataAccess'

    # When
    arn = im.get_role_arn(role_name)

    # Then
    assert arn is not None

def test_get_policies(im):
    pprint(im.get_policies())
    assert im.get_policies()


def test_get_redundant_policies(im):
    pprint(im.get_redundant_policies())
    assert im.get_redundant_policies()
