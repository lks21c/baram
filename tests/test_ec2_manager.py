from pprint import pprint

import pytest

from baram.ec2_manager import EC2Manager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def em():
    return EC2Manager()


def test_list_security_groups(em):
    pprint(em.list_security_groups())
    assert em.list_security_groups()


def test_list_instances(em):
    pprint(em.list_instances())
    assert type(em.list_instances()) == set


def test_list_subnet(em):
    pprint(em.list_subnet())


def test_delete_redundant_security_groups(em):
    redundant_security_group_ids = em.list_redundant_security_group_ids()
    em.delete_redundant_security_groups(redundant_security_group_ids)


def test_list_redundant_security_group_ids(em):
    sm = SagemakerManager()
    sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]
    pprint(em.list_redundant_security_group_ids(sm_domain_ids))
    assert type(em.list_redundant_security_group_ids()) == set


def test_list_vpc_sg_eni_subnets(em):
    result = em.list_vpc_sg_eni_subnets()
    pprint(result)
    assert type(result) == list
    assert len(result) == len(em.list_security_groups())
    if len(result) != 0:
        assert type(result[0]) == dict


def test_get_sg_ids_with_vpc_id(em):
    vpc_id = 'vpc-04863aaf1ea9a3b35'
    pprint(em.get_sg_ids_with_vpc_id(vpc_id))


def test_get_eni_ids_with_sg_id(em):
    sg_id = 'sg-0c41c743caf85d50b'
    pprint(em.get_eni_ids_with_sg_id(sg_id))


def test_list_security_group_relation(em):
    pprint(em.list_security_group_relations())
    assert em.list_security_group_relations()


def test_get_related_security_groups(em):
    sg_id = 'sg-0cc39dfc07c29ba7e'
    pprint(em.get_related_security_groups(sg_id))
    assert type(em.get_related_security_groups(sg_id)) == set


def test_get_security_group_rule(em):
    sg_id = 'sg-091f94833154ca3fd'
    pprint(em.get_security_group_rules(sg_id))
    assert type(em.get_security_group_rules(sg_id)) == list


def test_revoke_security_group_rule(em):
    em.revoke_security_group_rule('sg-04689e68b9eace024')


def test_delete_security_group(em):
    em.delete_security_group('sg-001f0a494205e0fe6')


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
