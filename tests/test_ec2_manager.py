from pprint import pprint

import pytest

from baram.ec2_manager import EC2Manager


@pytest.fixture()
def em():
    return EC2Manager()


def test_list_security_groups(em):
    for g_id in em.list_security_groups():
        print(g_id['GroupName'])
    assert em.list_security_groups()


def test_list_instances(em):
    pprint(em.list_instances())


def test_delete_redundant_key_pairs(em):
    em.delete_redundant_key_pairs()


def test_list_redundant_key_pairs(em):
    pprint(em.list_redundant_key_pairs())


def test_list_key_pairs(em):
    pprint(em.list_key_pairs())


def test_list_vpcs(em):
    pprint(em.list_vpcs())
    assert em.list_vpcs()


def test_list_subnets(em):
    for s in em.list_subnet():
        if 'Tags' in s:
            print(s['Tags'])
    assert em.list_subnet()


def test_get_ec2_instances_with_imds_v1(em):
    # When
    instance_list = em.get_ec2_instances_with_imds_v1()

    # Then
    pprint(instance_list)


def test_apply_imdsv2_only_mode(em):
    # Given
    instance_list = em.get_ec2_instances_with_imds_v1()

    # When
    em.apply_imdsv2_only_mode(instance_list)

    # Then
    response = em.get_ec2_instances_with_imds_v1()
    assert len(response) == 0
    print(response)
