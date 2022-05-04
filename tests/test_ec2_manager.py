from pprint import pprint

import pytest

from baram.airflow_manager import AirflowManager


@pytest.fixture()
def am():
    return AirflowManager()


def test_get_environment(am):
    response = am.get_environment('AFlowEnv')
    assert response
    pprint(response)


def test_update_environment(am):
    pass
    # am.update_environment('AFlowEnv')
