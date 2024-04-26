from pprint import pprint

import pytest

from baram.iam_manager import IAMManager
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
    assert type(response) == dict
    assert response['UserProfileName'] == user_profile_name
    assert list(response.keys()) == ['DomainId', 'UserProfileArn', 'UserProfileName', 'HomeEfsFileSystemUid', 'Status',
                                     'LastModifiedTime', 'CreationTime', 'UserSettings', 'ResponseMetadata']
    pprint(response)


def test_list_apps(sm):
    # When
    response = sm.list_apps()

    # Then
    assert type(response) == list
    for i in response:
        assert list(i.keys()) == ['DomainId', 'UserProfileName', 'AppType', 'AppName', 'Status', 'CreationTime']
    pprint(response)


def test_delete_app(sm):
    # Given
    user_profile_name = 'test-user'
    app_name = 'default'
    app_type = 'JupyterServer'

    # When
    response = sm.delete_app(user_profile_name=user_profile_name,
                             app_name=app_name,
                             app_type=app_type)
    # Then
    pprint(response)


def test_describe_app(sm):
    # Given
    user_profile_name = 'yoonje-lee'
    app_name = 'default'
    app_type = 'JupyterServer'

    # When
    response = sm.describe_app(user_profile_name=user_profile_name,
                               app_name=app_name,
                               app_type=app_type)
    # Then
    assert type(response) == dict
    assert response['UserProfileName'] == user_profile_name
    assert response['AppName'] == app_name
    assert response['AppType'] == app_type
    pprint(response)


def test_create_user_profile(sm):
    # Given
    im = IAMManager()
    user_profile_name = 'test-user'
    execution_role = im.get_role_arn(role_name='smbeta-execution-scientist-role')

    # When
    sm.create_user_profile(user_profile_name=user_profile_name,
                           execution_role=execution_role)

    # Then
    assert 'test-user' in [x['UserProfileName'] for x in sm.list_user_profiles()]
    pprint(sm.describe_user_profile(user_profile_name=user_profile_name))


def test_delete_user_profile(sm):
    # Given
    user_profile_name = 'test-user'

    # When
    response = sm.delete_user_profile(user_profile_name=user_profile_name)

    # Then
    assert user_profile_name not in [x['UserProfileName'] for x in sm.list_user_profiles()]
    pprint(response)


# TODO
def test_recreate_all_user_profiles(sm):
    pass
    # Given

    # When

    # Then


def test_list_domains(sm):
    # When
    response = sm.list_domains()

    # Then
    assert type(response) == list
    assert list(response[0].keys()) == ['DomainArn', 'DomainId', 'DomainName', 'Status', 'CreationTime',
                                        'LastModifiedTime', 'Url']
    pprint(response)


def test_get_domain_id(sm):
    # Given
    domain_name = 'smprod-domain'

    # When
    response = sm.get_domain_id(domain_name=domain_name)

    # Then
    assert type(response) == str
    pprint(response)


# TODO
def delete_domain(sm):
    pass
    # Given

    # When

    # Then


# TODO
def create_domain(sm):
    pass
    # Given

    # When

    # Then


# TODO: create a sample image and check it
def test_describe_image(sm):
    pass
    # response = sm.describe_image('sli-docker')
    # assert response
    # pprint(response)


# TODO: create a sample image and check it
def test_describe_image_version(sm):
    pass
    # response = sm.describe_image_version('sli-docker')
    # assert response['Version']
    # pprint(response['Version'])


# TODO
def test_create_image_version(sm):
    pass


# TODO
def test_delete_image(sm):
    pass


# TODO
def test_delete_image_version(sm):
    pass
