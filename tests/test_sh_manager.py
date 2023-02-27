from pprint import pprint

import pytest

from baram.sh_manager import SHManager


@pytest.fixture()
def sh():
    return SHManager()


def test_list_findings(sh):
    pprint(sh.list_findings())


def test_export_to_google_sheet(sh):
    pass
