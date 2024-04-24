from pprint import pprint

import pytest

from baram.glue_manager import GlueManager


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-dlprod-public')


@pytest.fixture()
def sample():
    return {'db_name': 'sample', 'table_name': 'sample_table'}


def test_get_databases(gm):
    # Given
    basic_databases = ['prod_mydata_master', 'sample']

    # When
    databases = gm.get_glue_databases()
    print(databases)

    # Then
    assert set(basic_databases).difference(set(databases)) == set()


def test_get_table(gm, sample):
    # Given
    db_name, table_name = sample['db_name'], sample['table_name']

    # When
    table_info = gm.get_table(db_name=db_name, table_name=table_name)

    # Then
    pprint(table_info['Table'])


def test_create_job(gm):
    gm.create_job('CreateHiraYearAgeInOut',
                  'dlbeta-public-developer-role',
                  'dlbeta-public-securityconfiguration')


def test_crud_job(gm):
    # Given
    job_name = 'test'

    # When/Then
    gm.create_job(job_name,
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration')

    pprint(gm.get_job('test'))

    gm.update_job(job_name,
                  'com.sli',
                  'Glue-developer-role',
                  's3://sli-dst-glue/sli-glue-1.0-SNAPSHOT.jar',
                  'sli-security-configuration')

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
