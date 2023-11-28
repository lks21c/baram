import json
import pytest
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


def test_get_role_arn(im):
    # Given
    role_name = 'AWSServiceRoleForLakeFormationDataAccess'

    # When
    arn = im.get_role_arn(role_name)

    # Then
    assert arn is not None


def test_get_user(im):
    # Given
    user_name = 'sagemaker-test'

    # When
    arn = im.get_user(user_name)

    # Then
    assert arn is not None


def test_get_user_arn(im):
    # Given
    user_name = 'sagemaker-test'

    # When
    arn = im.get_user_arn(user_name)

    # Then
    assert arn is not None


def test_list_policies(im):
    # When
    policies = im.list_policies()

    # Then
    if len(policies) > 0:
        assert 'PolicyId' in policies[0]
    else:
        print('There is no policy')
    print(json.dumps(policies, indent=4, default=str))


def test_list_redundant_policies(im):
    # When
    redundant_policies = im.list_redundant_policies()

    # Then
    if len(redundant_policies) > 0:
        assert 'PolicyId' in redundant_policies[0]
    else:
        print('There is no redundant policy')
    print(json.dumps(redundant_policies, indent=4, default=str))
