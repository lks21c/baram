from pprint import pprint

import pytest

from baram.glue_manager import GlueManager


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-glue')


def test_crud_job(gm):
    gm.create_job('test',
                  'com.sli.CreateHiraYearAgeGndr',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )

    pprint(gm.get_job('test'))

    gm.update_job('test',
                  'com.sli.CreateHiraYearAgeGndr',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )

    gm.delete_job('test')


def test_refresh_job(gm):
    pass
    # gm.refresh_job()
