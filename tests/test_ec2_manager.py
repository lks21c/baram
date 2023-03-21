import json
from pprint import pprint

import pytest

from baram.ec2_manager import EC2Manager
from baram.efs_manager import EFSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def em():
    return EC2Manager()


def test_list_sgs(em):
    # When
    sgs = em.list_sgs()
    pprint(sgs)

    # Then
    assert type(sgs) == list
    if len(sgs) > 0:
        assert type(sgs[0]) == dict
        assert 'GroupId' in sgs[0].keys()


def test_list_unused_sg_ids(em):
    # Given
    sm = SagemakerManager()
    sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    # When
    unused_sg_ids = em.list_unused_sg_ids('NFS', sm_domain_ids)
    pprint(unused_sg_ids)

    # Then
    if unused_sg_ids is not None:
        assert type(unused_sg_ids) == set


def test_list_vpc_sg_eni_subnets(em):
    # When
    vpc_sg_eni_subnets = em.list_vpc_sg_eni_subnets()
    pprint(vpc_sg_eni_subnets)

    # Then
    if vpc_sg_eni_subnets is not None:
        assert type(vpc_sg_eni_subnets) == list
        if len(vpc_sg_eni_subnets) > 0:
            assert type(vpc_sg_eni_subnets[0]) == dict


def test_get_default_vpc(em):
    # When
    default_vpc = em.get_default_vpc()

    # Then
    assert default_vpc['IsDefault']


def test_get_sg_ids_with_vpc_id(em):
    # Given
    default_vpc_id = em.get_default_vpc()['VpcId']

    # When
    sg_ids = em.get_sg_ids_with_vpc_id(default_vpc_id)
    pprint(sg_ids)

    # Then
    if sg_ids is not None:
        assert type(sg_ids) == list
        if len(sg_ids) > 0:
            assert type(sg_ids[0]) == str


def test_get_eni_with_sg_id(em):
    # Given
    default_vpc = em.get_default_vpc()
    default_sg_id = em.get_sg_ids_with_vpc_id(default_vpc['VpcId'][0])

    # When
    eni = em.get_eni_with_sg_id(default_sg_id)
    pprint(eni)

    # Then
    if eni is not None:
        assert type(eni) == list
        if len(eni) > 0:
            assert type(eni[0]) == dict


def test_list_sg_relations(em):
    # When
    sg_relations = em.list_sg_relations()
    pprint(em.list_sg_relations())

    # Then
    assert type(sg_relations) == list
    if len(sg_relations) > 0:
        assert type(sg_relations[0]) == dict
        assert 'sg_id' in sg_relations[0].keys()
        assert 'is_egress' in sg_relations[0].keys()
        assert 'related_sg_id' in sg_relations[0].keys()


def test_get_related_sgs(em):
    # Given
    default_vpc = em.get_default_vpc()
    default_sg_id = em.get_sg_ids_with_vpc_id(default_vpc['VpcId'][0])

    # When
    related_sgs = em.get_related_sgs(default_sg_id)

    # Then
    assert type(related_sgs) == set


def test_get_sg_rules(em):
    # Given
    default_vpc = em.get_default_vpc()
    default_sg_id = em.get_sg_ids_with_vpc_id(default_vpc['VpcId'][0])

    # When
    sg_rules = em.get_sg_rules(default_sg_id)
    pprint(sg_rules)

    # Then
    assert type(sg_rules) == list


def test_delete_sg_rules(em):
    # Given
    default_vpc = em.get_default_vpc()
    default_sg_id = em.get_sg_ids_with_vpc_id(default_vpc['VpcId'][0])

    # When
    em.delete_sg_rules(default_sg_id)

    # Then
    assert em.get_sg_rules(default_sg_id) == []


def test_delete_sgs(em):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    efsm = EFSManager()
    redundant_efs_ids = efsm.list_redundant_efs(redundant_sm_domain_ids)
    for efs_id in redundant_efs_ids:
        efsm.delete_efs(efs_id)

    unused_sg_ids = em.list_unused_sg_ids('NFS', redundant_sm_domain_ids)
    pprint(unused_sg_ids)

    # When
    em.delete_sgs(unused_sg_ids)

    # Then
    sg_ids = set([sg['GroupId'] for sg in em.list_sgs()])
    if unused_sg_ids is not None:
        assert unused_sg_ids - sg_ids == set()


def test_delete_sg(em):
    # Given
    vpc = em.get_default_vpc()
    sg_id = em.get_sg_ids_with_vpc_id(vpc['VpcId'][0])

    # When
    em.delete_sg(sg_id)

    # Then
    if not vpc['IsDefault']:
        sg_ids = set([sg['GroupId'] for sg in em.list_sgs()])
        assert sg_id not in sg_ids


def test_list_instances_with_status(em):
    # When
    instances_with_status = em.list_instances_with_status()
    pprint(instances_with_status)

    # Then
    if instances_with_status is not None:
        assert type(instances_with_status) == list


def test_delete_unused_key_pairs(em):
    # When
    em.delete_unused_key_pairs()

    # Then
    assert em.list_unused_key_pairs() == set()


def test_list_unused_key_pairs(em):
    # When
    unused_key_pairs = em.list_unused_key_pairs()
    pprint(unused_key_pairs)

    # Then
    assert type(unused_key_pairs) == set


def test_list_key_pairs(em):
    # When
    key_pairs = em.list_key_pairs()
    pprint(key_pairs)

    # Then
    assert type(key_pairs) == set


def test_list_vpcs(em):
    # When
    vpcs = em.list_vpcs()

    # Then
    assert type(vpcs) == list
    if len(vpcs) > 0:
        assert 'VpcId' in vpcs[0].keys()
    print(json.dumps(vpcs))


def test_list_detail_vpcs(em):
    # When
    vpcs = em.list_detail_vpcs()

    # When
    assert type(vpcs) == list
    assert len(vpcs) > 0
    for vpc in vpcs:
        print(vpc)


def test_list_subnets(em):
    # When
    subnets = em.list_subnets()
    pprint(subnets)

    # Then
    assert type(subnets) == list
    if len(subnets) > 0:
        assert type(subnets[0]) == dict


def test_list_detail_subnets(em):
    # When
    subnets = em.list_detail_subnets()
    pprint(subnets)

    # Then
    assert type(subnets) == list
    assert len(subnets) > 0


def test_get_subnet_ids_with_vpc_id(em):
    # Given
    default_vpc_id = em.get_default_vpc()['VpcId']

    # When
    subnets = em.get_subnet_ids_with_vpc_id(default_vpc_id)

    # Then
    assert len(subnets) > 0
    pprint(subnets)

def test_list_enis(em):
    # When
    enis = em.list_enis
    pprint(enis)

    # Then
    assert em.list_enis()


def test_describe_instances(em):
    pprint(em.describe_instances())
    assert em.describe_instances()


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
