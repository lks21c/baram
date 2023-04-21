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


def test_list_roles(im):
    # When
    roles = im.list_roles()

    # Then
    if len(roles) > 0:
        assert 'RoleId' in roles[0]
    else:
        print('There is no role')
    print(json.dumps(roles, indent=4, default=str))


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


def test_list_policies(im):
    # When
    policies = im.list_policies()

    # Then
    if len(policies) > 0:
        assert 'PolicyId' in policies[0]
    else:
        print('There is no policy')
    print(json.dumps(policies, indent=4, default=str))


def test_list_unused_policies(im):
    # When
    unused_policies = im.list_unused_policies()

    # Then
    if len(unused_policies) > 0:
        assert 'PolicyId' in unused_policies[0]
    else:
        print('There is no redundant policy')
    print(json.dumps(unused_policies, indent=4, default=str))


def test_delete_unused_policies(im):
    # To-do
    pass
