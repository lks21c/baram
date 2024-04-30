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
            'glue_job_type': 'glueetl',
            'num_of_dpus': float(2),
            'security_conf_name': 'dlbeta-public-securityconfiguration',
            'library_path': 's3://sli-dst-glue/library/glue_library.zip',
            'module': 'baram==0.4.1',
            'module2': 'dolbaram',
            'enable_iceberg': True,
            }


@pytest.fixture()
def sample2():
    return {'job_name': 'sample_glue_job_python',
            'role_name': 'dlbeta-public-developer-role',
            'glue_job_type': 'pythonshell',
            'num_of_dpus': float(1 / 16),
            'security_conf_name': 'dlbeta-public-securityconfiguration',
            'library_path': 's3://sli-dst-glue/library/glue_library.py',
            'module': 'baram==0.4.1,dolbaram',
            'enable_iceberg': True,
            }


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
                  num_of_dpus=sample['num_of_dpus'],
                  glue_job_type=sample['glue_job_type'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name'])
    pprint(ret)
    gm.delete_job(sample['job_name'])
    assert gm.get_job(sample['job_name']) is None


def test_create_job2(gm, sample2):
    # When
    gm.create_job(job_name=sample2['job_name'],
                  role_name=sample2['role_name'],
                  num_of_dpus=sample2['num_of_dpus'],
                  glue_job_type=sample2['glue_job_type'],
                  glue_security_conf_name=sample2['security_conf_name'],
                  python_library_path=sample2['library_path'],
                  python_module=sample2['module'],
                  enable_iceberg=sample2['enable_iceberg'])

    # Then
    ret = gm.get_job(sample2['job_name'])
    pprint(ret)
    gm.delete_job(sample2['job_name'])
    assert gm.get_job(sample2['job_name']) is None


def test_update_job(gm, sample):
    # Given
    gm.create_job(job_name=sample['job_name'],
                  role_name=sample['role_name'],
                  glue_job_type=sample['glue_job_type'],
                  num_of_dpus=sample['num_of_dpus'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])
    ret = gm.get_job(sample['job_name'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample['job_name'],
                  glue_job_type=sample['glue_job_type'],
                  python_library_path=f"{sample['library_path']}_",
                  num_of_dpus=int(sample['num_of_dpus'] * 2),
                  python_module=f"{sample['module']}_",
                  enable_iceberg=not sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == sample['glue_job_type']
    assert ret['Job']['AllocatedCapacity'] == int(sample['num_of_dpus'] * 2)
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample['module']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') is None

    gm.delete_job(sample['job_name'])
    assert gm.get_job(sample['job_name']) is None


def test_update_job2(gm, sample2):
    # Given
    gm.create_job(job_name=sample2['job_name'],
                  role_name=sample2['role_name'],
                  glue_job_type=sample2['glue_job_type'],
                  num_of_dpus=sample2['num_of_dpus'],
                  glue_security_conf_name=sample2['security_conf_name'],
                  python_library_path=sample2['library_path'],
                  python_module=sample2['module'],
                  enable_iceberg=sample2['enable_iceberg'])
    ret = gm.get_job(sample2['job_name'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample2['job_name'],
                  glue_job_type=sample2['glue_job_type'],
                  python_library_path=f"{sample2['library_path']}_",
                  num_of_dpus=int(sample2['num_of_dpus'] * 2),
                  python_module=f"{sample2['module']}_",
                  enable_iceberg=not sample2['enable_iceberg'])

    # Then
    ret = gm.get_job(sample2['job_name'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == sample2['glue_job_type']
    assert ret['Job']['AllocatedCapacity'] == int(sample2['num_of_dpus'] * 2)
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample2['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample2['module']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') is None

    gm.delete_job(sample2['job_name'])
    assert gm.get_job(sample2['job_name']) is None


def test_update_job3(gm, sample, sample2):
    # Given
    dpu_change = float(1 / 16)
    gm.create_job(job_name=sample['job_name'],
                  role_name=sample['role_name'],
                  glue_job_type=sample['glue_job_type'],
                  num_of_dpus=sample['num_of_dpus'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])
    ret = gm.get_job(sample['job_name'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample['job_name'],
                  glue_job_type=sample2['glue_job_type'],
                  python_library_path=f"{sample2['library_path']}_",
                  num_of_dpus=dpu_change,
                  python_module=f"{sample2['module']}_",
                  enable_iceberg=not sample2['enable_iceberg'])

    # Then
    ret = gm.get_job(sample['job_name'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == sample2['glue_job_type']
    assert ret['Job']['MaxCapacity'] == dpu_change
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample2['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample2['module']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') is None

    gm.delete_job(sample2['job_name'])
    assert gm.get_job(sample2['job_name']) is None


def test_update_job4(gm, sample, sample2):
    # Given
    gm.create_job(job_name=sample2['job_name'],
                  role_name=sample2['role_name'],
                  glue_job_type=sample2['glue_job_type'],
                  num_of_dpus=sample2['num_of_dpus'],
                  glue_security_conf_name=sample2['security_conf_name'],
                  python_library_path=sample2['library_path'],
                  python_module=sample2['module'],
                  enable_iceberg=sample2['enable_iceberg'])
    ret = gm.get_job(sample2['job_name'])
    assert ret['Job']['Name']

    # When
    gm.update_job(job_name=sample2['job_name'],
                  glue_job_type=sample['glue_job_type'],
                  python_library_path=f"{sample['library_path']}_",
                  num_of_dpus=int(sample['num_of_dpus'] * 2),
                  python_module=f"{sample['module']}_",
                  enable_iceberg=not sample['enable_iceberg'])

    # Then
    ret = gm.get_job(sample2['job_name'])
    pprint(ret)
    assert ret['Job']['Command']['Name'] == sample['glue_job_type']
    assert ret['Job']['AllocatedCapacity'] == int(sample['num_of_dpus'] * 2)
    assert ret['Job'].get('DefaultArguments', {}).get('--extra-py-files') == f"{sample['library_path']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--additional-python-modules') == f"{sample['module']}_"
    assert ret['Job'].get('DefaultArguments', {}).get('--datalake-formats') is None

    gm.delete_job(sample2['job_name'])
    assert gm.get_job(sample2['job_name']) is None


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


def test_rename_job(gm, sample):
    # Given
    old_job_name = sample['job_name']
    new_job_name = f"{sample['job_name']}_renamed"

    # Create a job with the old name
    gm.create_job(job_name=old_job_name,
                  role_name=sample['role_name'],
                  glue_job_type=gm.GLUE_TYPE_ETL,
                  num_of_dpus=sample['num_of_dpus'],
                  glue_security_conf_name=sample['security_conf_name'],
                  python_library_path=sample['library_path'],
                  python_module=sample['module'],
                  enable_iceberg=sample['enable_iceberg'])

    # When
    gm.rename_job(old_name=old_job_name, new_name=new_job_name)

    # Then
    assert gm.get_job(old_job_name) is None
    new_job = gm.get_job(new_job_name)
    assert new_job is not None
    print(new_job)

    # Cleanup
    gm.delete_job(new_job_name)
    assert gm.get_job(new_job_name) is None


def test_rename_job2(gm, sample2):
    # Given
    old_job_name = sample2['job_name']
    new_job_name = f"{sample2['job_name']}_renamed"

    # Create a job with the old name
    gm.create_job(job_name=old_job_name,
                  role_name=sample2['role_name'],
                  glue_job_type=gm.GLUE_TYPE_ETL,
                  num_of_dpus=sample2['num_of_dpus'],
                  glue_security_conf_name=sample2['security_conf_name'],
                  python_library_path=sample2['library_path'],
                  python_module=sample2['module'],
                  enable_iceberg=sample2['enable_iceberg'])

    # When
    gm.rename_job(old_name=old_job_name, new_name=new_job_name)

    # Then
    assert gm.get_job(old_job_name) is None
    new_job = gm.get_job(new_job_name)
    assert new_job is not None
    print(new_job)

    # Cleanup
    gm.delete_job(new_job_name)
    assert gm.get_job(new_job_name) is None
