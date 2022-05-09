from pprint import pprint

import pytest

from baram.kms_manager import KMSManager
from baram.s3_manager import S3Manager
from baram.glue_manager import GlueManager


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-glue')


def test_create_job(gm):
    # pass
    gm.create_job('CreateHiraYearAgeInOut',
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )


def test_delete_job(gm):
    # pass
    gm.delete_job('CreateHiraYearAgeInOut')
    gm.delete_table('sli-dst', 'hira_year_age_gndr')
    sm = S3Manager('sli-dst-glue', KMSManager().get_kms_arn('s3-glue-ksm'))
    sm.delete_dir('table/hira_year_age_gndr')


def test_crud_job(gm):
    gm.create_job('test',
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration'
                  )

    pprint(gm.get_job('test'))

    gm.update_job('test',
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
