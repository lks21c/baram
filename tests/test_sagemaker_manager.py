import time
from pprint import pprint

import pytest

from baram.iam_manager import IAMManager
from baram.ec2_manager import EC2Manager
from baram.kms_manager import KMSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def sm():
    return SagemakerManager(domain_name='smbeta-domain')


@pytest.fixture()
def im():
    return IAMManager()


@pytest.fixture()
def em():
    return EC2Manager()


@pytest.fixture()
def km():
    return KMSManager()


def test_get_domain_id(sm):
    # Given
    domain_name = 'smbeta-domain'

    # When
    response = sm.get_domain_id(domain_name=domain_name)

    # Then
    assert type(response) == str
    pprint(response)


def test_list_domains(sm):
    # When
    response = sm.list_domains()

    # Then
    assert type(response) == list
    pprint(response)


def test_create_describe_delete_domain(sm, em, im, km):
    # Given
    domain_name = 'temp-domain'
    auth_mode = 'IAM'
    execution_role_arn = im.get_role_arn('smbeta-execution-engineer-iam-role')
    sg_groups = [em.get_sg_id_with_sg_name('beta-public-vpc-default-sg')]
    vpc_id = em.get_vpc_id_with_vpc_name('beta-public-vpc')
    subnet_names = ['beta-public-vpc-pub-sub1', 'beta-public-vpc-pub-sub1']
    subnet_ids = [em.get_subnet_id(vpc_id, x) for x in subnet_names]
    app_network_access_type = 'PublicInternetOnly'
    efs_kms_id = km.get_kms_arn('smbeta-public-s3-kms', False)

    # When/Then
    sm.create_domain(domain_name=domain_name,
                     auth_mode=auth_mode,
                     execution_role_arn=execution_role_arn,
                     sg_groups=sg_groups,
                     subnet_ids=subnet_ids,
                     vpc_id=vpc_id,
                     app_network_access_type=app_network_access_type,
                     efs_kms_id=efs_kms_id)

    assert domain_name in [x['DomainName'] for x in sm.list_domains()]

    domain_id = sm.get_domain_id(domain_name)
    response = sm.describe_domain(domain_id=domain_id)
    assert response['Status'] in ['Pending', 'InService']
    pprint(response)

    sm.delete_domain(domain_id=domain_id,
                     delete_user_profiles=False)
    assert sm.describe_domain(domain_id=domain_id) is None


def test_list_user_profiles(sm):
    # When
    response = sm.list_user_profiles()

    # Then
    assert type(response) == list
    pprint(response)


def test_create_describe_delete_user_profile(sm, im):
    # Given
    user_profile_name = 'temp-user'
    execution_role = im.get_role_arn(role_name='smbeta-execution-scientist-role')

    # When/Then
    sm.create_user_profile(user_profile_name=user_profile_name,
                           execution_role=execution_role)

    assert user_profile_name in [x['UserProfileName'] for x in sm.list_user_profiles()]

    response = sm.describe_user_profile(user_profile_name=user_profile_name)
    assert type(response) == dict
    assert response['UserProfileName'] == user_profile_name
    assert response['Status'] == 'InService'
    pprint(response)

    sm.delete_user_profile(user_profile_name=user_profile_name)
    assert sm.describe_user_profile(user_profile_name=user_profile_name) is None


def test_recreate_all_user_profiles(sm):
    # Given
    old_user_profiles = [x['UserProfileName'] for x in sm.list_user_profiles()]
    old_user_profile_execution_roles = [sm.describe_user_profile(user_profile_name=x)
                                        ['UserSettings']['ExecutionRole'] for x in old_user_profiles]

    # When
    sm.recreate_all_user_profiles()
    new_user_profiles = [x['UserProfileName'] for x in sm.list_user_profiles()]
    new_user_profile_execution_roles = [sm.describe_user_profile(user_profile_name=x)
                                        ['UserSettings']['ExecutionRole'] for x in new_user_profiles]

    # Then
    assert len(old_user_profiles) == len(new_user_profiles)
    assert old_user_profiles.sort() == new_user_profiles.sort()
    assert old_user_profile_execution_roles.sort() == new_user_profile_execution_roles.sort()


def test_list_apps(sm):
    # When
    response = sm.list_apps()

    # Then
    assert type(response) == list
    pprint(response)


def test_create_describe_delete_app(sm, im):
    # Given
    user_profile_name = 'temp-user'
    execution_role = im.get_role_arn(role_name='smbeta-execution-scientist-role')
    app_type = 'JupyterServer'
    app_name = 'temp'
    sm.create_user_profile(user_profile_name=user_profile_name,
                           execution_role=execution_role)

    # When/Then
    sm.create_app(user_profile_name=user_profile_name,
                  app_type=app_type,
                  app_name=app_name)

    response = sm.describe_app(user_profile_name=user_profile_name,
                               app_name=app_name,
                               app_type=app_type)

    assert type(response) == dict
    assert response['UserProfileName'] == user_profile_name
    assert response['AppName'] == app_name
    assert response['AppType'] == app_type
    pprint(response)

    sm.delete_app(user_profile_name=user_profile_name,
                  app_name=app_name,
                  app_type=app_type)
    assert sm.describe_app(user_profile_name=user_profile_name, app_name=app_name,
                           app_type=app_type)['Status'] == 'Deleted'

    sm.delete_user_profile(user_profile_name=user_profile_name)


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


def test_create_describe_delete_image(sm, im):
    # Given
    image_name = 'temp-image'
    role_arn = im.get_role_arn(role_name='smbeta-execution-engineer-iam-role')

    # When
    sm.create_image(image_name=image_name,
                    role_arn=role_arn)

    # Then
    response = sm.describe_image(image_name=image_name)
    assert response['ImageName'] == image_name
    assert response['ImageStatus'] == 'CREATED'
    pprint(response)

    sm.delete_image(image_name=image_name)
    assert sm.describe_image(image_name=image_name) is None


def test_create_describe_delete_image_version(sm, im):
    # Given
    image_name = 'temp-image'
    role_arn = im.get_role_arn(role_name='smbeta-execution-engineer-iam-role')
    # TODO: change to infer image url
    base_image_url = '145885190059.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker_image:latest'
    sm.create_image(image_name=image_name, role_arn=role_arn)

    # When/Then
    sm.create_image_version(base_image_uri=base_image_url,
                            image_name=image_name)

    response = sm.describe_image_version(image_name)
    assert response['BaseImage'] == base_image_url
    assert response['ImageVersionStatus'] == 'CREATED'
    pprint(response)

    version = sm.describe_image_version(image_name)['Version']
    sm.delete_image_version(image_name=image_name, version=version)
    assert sm.describe_image_version(image_name=image_name, Version=version) is None

    sm.delete_image(image_name=image_name)
