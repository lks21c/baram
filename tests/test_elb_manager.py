from pprint import pprint

import pytest
from baram.elb_manager import ELBManager


@pytest.fixture()
def elb():
    return ELBManager()


def test_describe_load_balancers(elb):
    pprint(elb.describe_load_balancers())


def test_delete_load_balancer(elb):
    elb.delete_load_balacner('')