from pprint import pprint

import pytest

from baram.glue_manager import GlueManager


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-glue')


def test_create_job(gm):
    gm.create_job('CreateHiraYearAgeInOut',
                  'dlbeta-public-developer-role',
                  'dlbeta-public-securityconfiguration')


def test_delete_table(gm):
    gm.delete_table('sli-dst', 'test', True)


def test_crud_job(gm):
    job_name = 'test'
    gm.create_job(job_name,
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )

    pprint(gm.get_job('test'))

    gm.update_job(job_name,
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )

    gm.delete_job('test')


def test_refresh_job(gm):
    pass
    # gm.refresh_job('src/main/scala/com/sli',
    #                ['GlueHelper'],
    #                'com.sli',
    #                'Glue-developer-role',
    #                's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
    #                'sli-security-configuration')


def test_summary(gm):
    gm.summary()
