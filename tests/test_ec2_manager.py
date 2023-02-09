from pprint import pprint

import pytest

from baram.ec2_manager import EC2Manager


@pytest.fixture()
def em():
    return EC2Manager()


def test_list_security_groups(em):
    pprint(em.list_security_groups())
    assert em.list_security_groups()


def test_list_instances(em):
    pprint(em.list_instances())
    assert em.list_instances()


def test_list_security_group_id_with_instances(em):
    pprint(em.list_security_group_id_with_instances())
    assert em.list_security_group_id_with_instances()


def test_list_security_group_id_without_instances(em):
    pprint(em.list_security_group_id_without_instances())
    assert em.list_security_group_id_without_instances()


def test_delete_redundant_sm_security_groups(em):
    em.delete_redundant_sm_security_groups()


def test_list_redundant_sm_security_group(em):
    pprint(em.list_redundant_sm_security_groups())
    assert em.list_redundant_sm_security_groups()


def test_list_security_group_relation(em):
    pprint(em.list_security_group_relations())
    assert em.list_security_group_relations()


def test_get_related_security_groups(em):
    sg_id = 'sg-0cc39dfc07c29ba7e'
    pprint(em.get_related_security_groups(sg_id))
    assert em.get_related_security_groups(sg_id)


def test_get_security_group_rule(em):
    sg_id = 'sg-0fe321a807f037ba1'
    pprint(em.get_security_group_rules(sg_id))
    assert type(em.get_security_group_rules(sg_id)) == list


def test_revoke_security_group_rule(em):
    em.revoke_security_group_rule('sg-0fe321a807f037ba1')


def test_delete_security_group(em):
    em.delete_security_group('sg-0fe321a807f037ba1')


def test_list_vpcs(em):
    pprint(em.list_vpcs())
    assert em.list_vpcs()


def test_list_subnets(em):
    for s in em.list_subnet():
        if 'Tags' in s:
            print(s['Tags'])
    assert em.list_subnet()


def test_get_sg_id(em):
    print(em.get_sg_id('AFlow'))
    assert em.get_sg_id('AFlow')


def test_get_vpc_id(em):
    print(em.get_vpc_id('aflow'))
    assert em.get_vpc_id('aflow')


def test_get_subnet_id(em):
    pprint(em.get_subnet_id(em.get_vpc_id('aflow'), 'AFlow-Pri-Sub-AZ1'))
    em.get_subnet_id(em.get_vpc_id('aflow'), 'AFlow-Pri-Sub-AZ1')


def test_get_ec2_id(em):
    ec2_id = em.get_ec2_id('bastion-host-1')
    assert ec2_id
    print(ec2_id)
