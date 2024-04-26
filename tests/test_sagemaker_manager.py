from pprint import pprint

import pytest

from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def sm():
    return SagemakerManager(domain_name='smprod-domain')


def test_list_user_profiles(sm):
    # When
    response = sm.list_user_profiles()

    # Then
    assert type(response) == list
    pprint(response)


def test_describe_user_profile(sm):
    # Given
    user_profile_name = 'test1'

    # When
    response = sm.describe_user_profile(user_profile_name=user_profile_name)

    # Then
    print(response)


def test_list_apps(sm):
    # When
    response = sm.list_apps()

    # Then
    pprint(response)


def delete_app(sm):
    pass
    # Given

    # When

    # Then


def describe_app(sm):
    pass
    # Given

    # When

    # Then


# def test_create_user_profile(sm):
#     # Given
#     user_profile_name = 'test4'
#     execution_role = 'arn:aws:iam::145885190059:role/smbeta-execution-scientist-role'
#
#     # When
#     sm.create_user_profile(user_profile_name=user_profile_name,
#                            execution_role=execution_role)
#
#
# def test_delete_user_profile(sm):
#     # Given
#     user_profile_name = 'test4'
#
#     # When
#     sm.delete_user_profile(user_profile_name=user_profile_name)


# def test_recreate_all_user_profiles(sm):
#     # When
#     sm.recreate_all_user_profiles()


# def test_describe_image(sm):
#     # TODO: create a sample image and check it
#     response = sm.describe_image('sli-docker')
#     assert response
#     pprint(response)


def test_list_domains(sm):
    # When
    response = sm.list_domains()

    # Then
    assert type(response) == list
    pprint(response)


def test_get_domain_id(sm):
    # Given
    domain_name = 'smbeta-domain'

    # When
    response = sm.get_domain_id(domain_name=domain_name)

    # Then
    assert type(response) == str
    pprint(response)


def delete_domain(sm):
    pass
    # Given

    # When

    # Then


def create_domain(sm):
    pass
    # Given

    # When

    # Then

# def test_describe_image_version(sm):
#     # TODO: create a sample image and check it
#     response = sm.describe_image_version('sli-docker')
#     assert response['Version']
#     pprint(response['Version'])
