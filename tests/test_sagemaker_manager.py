from pprint import pprint

import pytest

from baram.iam_manager import IAMManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def sm():
    return SagemakerManager(domain_name='smbeta-domain')


@pytest.fixture()
def im():
    return IAMManager()


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
    user_profile_name = 'test1'
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


def test_create_user_profile(sm, im):
    # Given
    user_profile_name = 'test1'
    execution_role = im.get_role_arn(role_name='smbeta-execution-scientist-role')

    # When
    sm.create_user_profile(user_profile_name=user_profile_name,
                           execution_role=execution_role)

    # Then
    assert user_profile_name in [x['UserProfileName'] for x in sm.list_user_profiles()]
    pprint(sm.describe_user_profile(user_profile_name=user_profile_name))


def test_delete_user_profile(sm):
    # Given
    user_profile_name = 'test1'

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
def test_delete_domain(sm):
    assert False
    # When
    # sm.delete_domain(delete_user_profiles=True)

    # Then


# TODO
def create_domain(sm):
    assert False
    # Given

    # When

    # Then


def test_list_images(sm):
    # When
    response = sm.list_images()

    # Then
    assert type(response) == list
    pprint(response)


def test_list_image_versions(sm):
    # Given
    image_name = 'sm-image'

    # When
    response = sm.list_image_versions(image_name=image_name)

    # Then
    assert type(response) == list
    for i in response:
        assert i['Version']
    pprint(response)


# TODO: create a sample image and check it
def test_describe_image(sm):
    # Given
    image_name = 'sm-image'

    # Given
    response = sm.describe_image(image_name)

    # Then
    assert image_name == response['ImageName']
    pprint(response)


# TODO: create a sample image and check it
def test_describe_image_version(sm):
    # Given
    image_name = 'sm-image'

    # When
    response = sm.describe_image_version(image_name)

    # THen
    assert response['ImageArn'].split('/')[-1] == image_name
    assert response['Version']
    pprint(response)


def test_create_image(sm, im):
    # Given
    image_name = 'sm-image'
    role_arn = im.get_role_arn(role_name='smbeta-execution-engineer-iam-role')

    # When
    sm.create_image(image_name=image_name,
                    role_arn=role_arn)

    # Then
    response = sm.describe_image(image_name=image_name)
    assert response['ImageName'] == image_name
    assert response['ImageStatus'] in ['CREATING', 'CREATED']


def test_create_image_version(sm):
    # Given
    base_image_url = '145885190059.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker_image:latest'
    image_name = 'sm-image'

    # When
    sm.create_image_version(base_image_uri=base_image_url,
                            image_name=image_name)

    # Then
    response = sm.describe_image_version(image_name)
    assert response['BaseImage'] == base_image_url
    assert response['ImageVersionStatus'] in ['CREATING', 'CREATED']
    pprint(response)


def test_delete_image(sm):
    # Given
    image_name = 'sm-image'

    # When
    sm.delete_image(image_name=image_name)

    # Then
    assert image_name not in sm.list_images()


def test_delete_image_version(sm):
    # Given
    image_name = 'sm-image'
    version = 3

    # When
    sm.delete_image_version(image_name=image_name, version=version)

    # Then
    # TODO
