from pprint import pprint

import pytest

from baram.efs_manager import EFSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def efsm():
    return EFSManager()


def test_list_efs(efsm):
    efs = efsm.list_efs()
    pprint(efs)
    assert type(efs) == list


# def test_list_mount_targets(efsm):
#     # TODO: add sample efs and check it
#     efs_id = 'fs-090f17ba800036e22'
#     pprint(efsm.list_mount_targets(efs_id))
#
#     # TODO: add assertion(s)


def test_delete_efs(efsm):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]
    redundant_efs_ids = efsm.list_redundant_efs(redundant_sm_domain_ids)

    # When
    for redundant_efs_id in redundant_efs_ids:
        efsm.delete_efs(redundant_efs_id)


def test_list_redundant_efs(efsm):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    # When
    redundant_efs_ids = efsm.list_redundant_efs(redundant_sm_domain_ids)

    # Then
    assert len(redundant_efs_ids) >= 0
    assert type(redundant_efs_ids) == list


def test_delete_mount_targets(efsm):
    # Given
    # TODO: replace this to non-specific efs id
    efs_id = 'fs-090f17ba800036e22'

    mount_targets = efsm.list_mount_targets(efs_id)

    if mount_targets is not None:
        mount_target_ids = [mt['MountTargetId'] for mt in mount_targets]
        for mt_id in mount_target_ids: efsm.delete_mount_targets(mt_id)

