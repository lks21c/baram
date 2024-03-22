import pytest
from pprint import pprint

from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def sm():
    return SagemakerManager(domain_id='d-dwdufhmgqavz')


def test_list_user_profiles(sm):
    response = sm.list_user_profiles()
    pprint(response)


def test_list_apps(sm):
    # Given
    user_profile_name = 'test1'

    # When
    response = sm.list_apps(UserProfileNameEquals=user_profile_name)

    # Then
    pprint(response)


def test_describe_user_profile(sm):
    # Given
    user_profile_name = 'test1'

    # When
    response = sm.describe_user_profile(user_profile_name=user_profile_name)

    # Then
    print(response)


# def test_create_user_profile(sm):
#     # Given
#     user_profile_name = 'test1'
#     execution_role = 'arn:aws:iam::145885190059:role/smbeta-execution-scientist-role'
#
#     # When
#     sm.create_user_profile(user_profile_name=user_profile_name,
#                            execution_role=execution_role)


# def test_delete_user_profile(sm):
#     # Given
#     user_profile_name = 'test1'
#
#     # When
#     sm.delete_user_profile(user_profile_name=user_profile_name)


# def test_recreate_all_user_profiles(sm):
#     # When
#     sm.recreate_all_user_profiles()


def test_describe_image(sm):
    response = sm.describe_image('sli-docker')
    assert response
    pprint(response)


def test_list_domains(sm):
    response = sm.list_domains()
    assert type(response) == list
    pprint(response)


def test_describe_image_version(sm):
    response = sm.describe_image_version('sli-docker')
    assert response['Version']
    pprint(response['Version'])
