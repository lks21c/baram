from pprint import pprint

import pytest

from baram.glue_manager import GlueManager


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-dlprod-public')


@pytest.fixture()
def sample():
    return {'job_name': 'sample_glue_job_glue',
            'job_name2': 'sample_glue_job_python',
            'role_name': 'dlbeta-public-developer-role',
            'security_conf_name': 'dlbeta-public-securityconfiguration',
            'library_path': 's3://sli-dst-glue/library/glue_library.zip',
            'module': 'baram==0.4.1',
            'module2': 'dolbaram',
            'enable_iceberg': True,
            }

    # Then
    ret = gm.get_job(sample['job_name'])
    assert ret['Job']['Name']
    pprint(ret)
    gm.delete_job(sample['job_name'])
    assert gm.get_job(sample['job_name']) is None


def test_get_databases(gm):
    # Given
    basic_databases = ['prod_mydata_master', 'sample']

    # When
    databases = gm.get_glue_databases()
    print(databases)

    # Then
    assert set(basic_databases).difference(set(databases)) == set()


def test_create_job(gm, sample):
    # When
    gm.create_job(job_name=sample['job_name'],
                  role_name=sample['role_name'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])


def test_create_job2(gm, sample):
    # When
    gm.create_job(job_name=sample['job_name2'],
                  role_name=sample['role_name'],
                  glue_job_type=gm.GLUE_TYPE_PYTHON_SHELL,
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name2'])
    pprint(ret)
    gm.delete_job(sample['job_name2'])
    assert gm.get_job(sample['job_name2']) is None


def test_update_job(gm, sample):
    # Given
    gm.create_job(job_name=sample['job_name'],
                  role_name=sample['role_name'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])
    ret = gm.get_job(sample['job_name'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample['job_name'],
                  glue_job_type=gm.GLUE_TYPE_PYTHON_SHELL,
                  python_library_path=f"{sample['library_path']}_",
                  python_module=f"{sample['module']}_",
                  enable_iceberg=not sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == gm.GLUE_TYPE_PYTHON_SHELL
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample['module']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') is None

    gm.delete_job(sample['job_name'])
    assert gm.get_job(sample['job_name']) is None


def test_update_job2(gm, sample):
    # Given
    gm.create_job(job_name=sample['job_name2'],
                  role_name=sample['role_name'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=not sample['enable_iceberg'])
    ret = gm.get_job(sample['job_name2'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample['job_name2'],
                  glue_job_type=gm.GLUE_TYPE_ETL,
                  python_library_path=f"{sample['library_path']}_",
                  python_module=f"{sample['module2']}",
                  enable_iceberg=sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name2'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == gm.GLUE_TYPE_ETL
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample['module2']}"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') == 'iceberg'

    # gm.delete_job(sample['job_name'])
    # assert gm.get_job(sample['job_name']) is None


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
