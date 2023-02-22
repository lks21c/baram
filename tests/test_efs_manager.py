from pprint import pprint

import pytest

from baram.efs_manager import EFSManager
from baram.sagemaker_manager import SagemakerManager


@pytest.fixture()
def efsm():
    return EFSManager()


def test_list_file_systems(efsm):
    file_systems = efsm.list_file_systems()
    pprint(file_systems)
    assert file_systems


def test_list_mount_targets(efsm):
    file_system_id = 'fs-090f17ba800036e22'
    pprint(efsm.list_mount_targets(file_system_id))


def test_delete_redundant_file_systems(efsm):
    # Given
    sm = SagemakerManager()
    redundant_sm_domain_ids = [domain['DomainId'] for domain in sm.list_domains()]

    # When
    efsm.delete_redundant_file_systems(redundant_sm_domain_ids)


def test_delete_mount_targets(efsm):
    file_system_id = 'fs-090f17ba800036e22'
    mount_target_ids = [mt['MountTargetId'] for mt in efsm.list_mount_targets(file_system_id)]

    for mt_id in mount_target_ids:
        efsm.delete_mount_targets(mt_id)


def test_delete_file_system(efsm):
    file_system_id = 'fs-01f416ca4949edcfc'
    efsm.delete_file_system(file_system_id)