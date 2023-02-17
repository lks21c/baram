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


def test_list_vpcs(em):
    pprint(em.list_vpcs())
    assert em.list_vpcs()


def test_list_subnets(em):
    for s in em.list_subnet():
        if 'Tags' in s:
            print(s['Tags'])
    assert em.list_subnet()

#
# def test_get_sg_id(em):
#     print(em.get_sg_id('AFlow'))
#     assert em.get_sg_id('AFlow')
#
#
# def test_get_vpc_id(em):
#     print(em.get_vpc_id('aflow'))
#     assert em.get_vpc_id('aflow')
#
#
# def test_get_subnet_id(em):
#     pprint(em.get_subnet_id(em.get_vpc_id('aflow'), 'AFlow-Pri-Sub-AZ1'))
#     em.get_subnet_id(em.get_vpc_id('aflow'), 'AFlow-Pri-Sub-AZ1')
#
#
# def test_get_ec2_id(em):
#     ec2_id = em.get_ec2_id('bastion-host-1')
#     assert ec2_id
#     print(ec2_id)
