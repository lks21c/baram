from pprint import pprint

import pytest

from baram.s3_manager import S3Manager
from baram.sagemaker_pipeline_manager import SagemakerPipelineManager


@pytest.fixture()
def sample_data():
    return {'pipeline_name': 'smbeta-pipeline',
            'default_s3_bucket': 'sli-dst-dlprod-public', }


@pytest.fixture()
def spm(sample_data):
    return SagemakerPipelineManager(default_bucket=sample_data['default_s3_bucket'],
                                    pipeline_name=sample_data['pipeline_name'],
                                    role_arn='arn:aws:iam::145885190059:role/smbeta-execution-engineer-iam-role',
                                    is_local_mode=True)


@pytest.fixture()
def sm(sample_data):
    return S3Manager(sample_data['default_s3_bucket'])


def test_upload_local_files(spm, sm):
    # When
    spm.upload_local_files('conf')

    # Then
    objs = sm.list_objects(f'{spm.pipeline_name}/conf/')
    for obj in objs:
        assert obj['Key'].endswith('a.json')


def test_create_single_sklearn_pipeline(spm, sample_data):
    # When
    spm.create_single_sklearn_pipeline(base_s3_uri=f'{sample_data["pipeline_name"]}/input/',
                                       code_s3_uri=f'{sample_data["pipeline_name"]}/code/preprocessing.py')

    # Then
    pipeline = spm.describe_pipeline(spm.pipeline_name)
    assert pipeline


def test_list_pipelines(spm):
    # When
    pipelines = spm.list_pipelines()

    # Then
    assert pipelines
    pprint(pipelines)


def test_describe_pipeline(spm):
    # When
    pipeline = spm.describe_pipeline(spm.pipeline_name)

    # Then
    assert pipeline
    pprint(pipeline)
