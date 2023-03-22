import json

import pytest

from baram.sh_manager import SHManager


@pytest.fixture()
def sh():
    return SHManager()


def test_list_findings(sh):
    # When
    findings = sh.list_findings()

    # Then
    print(json.dumps(findings, indent=4))


def test_export_to_google_sheet(sh):
    pass
