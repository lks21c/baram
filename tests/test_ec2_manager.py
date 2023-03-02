from pprint import pprint

import pytest

from baram.ec2_manager import EC2Manager
from baram.efs_manager import EFSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def em():
    return EC2Manager()


def test_list_sgs(em):
    pprint(em.list_sgs())
    assert em.list_sgs()


def test_list_unused_sg_ids(em):
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    pprint(em.list_unused_sg_ids(redundant_sm_domain_ids))
    assert type(em.list_unused_sg_ids()) == set


def test_list_vpc_sg_eni_subnets(em):
    result = em.list_vpc_sg_eni_subnets()
    pprint(result)
    assert type(result) == list
    assert len(result) == len(em.list_sgs())
    if len(result) != 0:
        assert type(result[0]) == dict


def test_get_sg_ids_with_vpc_id(em):
    default_vpc_id = [vpc for vpc in em.list_vpcs() if vpc['IsDefault']][0]['VpcId']
    pprint(em.get_sg_ids_with_vpc_id(default_vpc_id))


def test_get_eni_with_sg_id(em):
    sg_id = 'sg-0c41c743caf85d50b'
    pprint(em.get_eni_with_sg_id(sg_id))


def test_list_sg_relations(em):
    pprint(em.list_sg_relations())
    assert em.list_sg_relations()


def test_get_related_sgs(em):
    sg_id = 'sg-0cc39dfc07c29ba7e'
    pprint(em.get_related_sgs(sg_id))
    assert type(em.get_related_sgs(sg_id)) == set


def test_get_sg_rules(em):
    pprint(em.get_sg_rules('sg-0817bb034de60ade5'))


def test_revoke_sg_rules(em):
    em.revoke_sg_rules('sg-0817bb034de60ade5')


def test_delete_sgs(em):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    efsm = EFSManager()
    redundant_efs_ids = efsm.list_redundant_efss(redundant_sm_domain_ids)
    for efs_id in redundant_efs_ids:
        efsm.delete_efs(efs_id)

    unused_sg_ids = em.list_unused_sg_ids(redundant_sm_domain_ids)
    pprint(unused_sg_ids)

    # When
    em.delete_sgs(unused_sg_ids)


def test_delete_sg(em):
    em.delete_sg('sg-001f0a494205e0fe6')


def test_list_vpcs(em):
    pprint(em.list_vpcs())
    assert em.list_vpcs()


def test_list_subnets(em):
    for s in em.list_subnets():
        if 'Tags' in s:
            print(s['Tags'])
    assert em.list_subnets()


def test_list_enis(em):
    pprint(em.list_enis())
    assert em.list_enis()


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
