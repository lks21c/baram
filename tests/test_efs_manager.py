from pprint import pprint

import pytest

from baram.efs_manager import EFSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def efsm():
    return EFSManager()


def test_list_efss(efsm):
    efss = efsm.list_efss()
    pprint(efss)
    assert efss


def test_list_mount_targets(efsm):
    efs_id = 'fs-090f17ba800036e22'
    pprint(efsm.list_mount_targets(efs_id))


def test_delete_efss(efsm):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]
    redundant_efs_ids = efsm.list_redundant_efss(redundant_sm_domain_ids)

    # When
    efsm.delete_efss(redundant_efs_ids)


def test_list_redundant_efss(efsm):
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]
    redundant_efs_ids = efsm.list_redundant_efss(redundant_sm_domain_ids)
    assert len(redundant_efs_ids) >= 0
    assert type(redundant_efs_ids) == list


def test_delete_mount_targets(efsm):
    efs_id = 'fs-090f17ba800036e22'
    mount_targets = efsm.list_mount_targets(efs_id)

    if mount_targets is not None:
        mount_target_ids = [mt['MountTargetId'] for mt in mount_targets]
        for mt_id in mount_target_ids: efsm.delete_mount_targets(mt_id)


def test_delete_efs(efsm):
    efs_id = 'fs-01f416ca4949edcfc'
    efsm.delete_efs(efs_id)
