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


def test_create_single_script_pipeline(spm, sample_data, sm):
    # Given
    filename = 'preprocessing.py'
    # TODO: to be deleted.
    sm.upload_file(f'/Users/lks21c/repo/baram/tests/{filename}', f'smbeta-pipeline/code/{filename}')

    # When
    spm.create_single_script_pipeline(
        ecr_image_uri='145885190059.dkr.ecr.ap-northeast-2.amazonaws.com/lks21c_sm_preprocess:241003',
        base_s3_uri=f'{sample_data["pipeline_name"]}/input/',
        code_s3_uri=f'{sample_data["pipeline_name"]}/code/{filename}',
        outputs=[
            spm.get_processing_output(output_name='train',
                                      source=f"{spm.sagemaker_processor_home}/train",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/train.csv'),
            spm.get_processing_output(output_name='validation',
                                      source=f"{spm.sagemaker_processor_home}/validation",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/validation.csv'),
            spm.get_processing_output(output_name='test',
                                      source=f"{spm.sagemaker_processor_home}/test",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/test.csv'),
        ]),
    spm.start_pipeline()

    # Then
    pipeline = spm.describe_pipeline(spm.pipeline_name)
    assert pipeline


def test_preprocess_train_pipeline(spm, sample_data, sm):
    # Given
    filename = 'preprocessing.py'
    sm.upload_file(f'/Users/lks21c/repo/baram/tests/{filename}', f'smbeta-pipeline/code/{filename}')
    instance_type = 'ml.m5.xlarge'

    # When
    s1 = spm.get_script_step(
        ecr_image_uri='145885190059.dkr.ecr.ap-northeast-2.amazonaws.com/lks21c_sm_preprocess:241003',
        base_s3_uri=f'{sample_data["pipeline_name"]}/input/',
        code_s3_uri=f'{sample_data["pipeline_name"]}/code/{filename}',
        outputs=[
            spm.get_processing_output(output_name='train',
                                      source=f"{spm.sagemaker_processor_home}/train",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/train.csv'),
            spm.get_processing_output(output_name='validation',
                                      source=f"{spm.sagemaker_processor_home}/validation",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/validation.csv'),
            spm.get_processing_output(output_name='test',
                                      source=f"{spm.sagemaker_processor_home}/test",
                                      destination=f's3://{spm.default_bucket}/{spm.pipeline_name}/output/test.csv'),
        ]
    )

    image_uri = spm.get_image_uri(framework='xgboost', version='1.0-1', instance_type=instance_type)
    h_params = {'objective': "reg:linear",
                'num_round': 50,
                'max_depth': 5,
                'eta': 0.2,
                'gamma': 4,
                'min_child_weight': 6,
                'subsample': 0.7,
                'silent': 0}

    s2 = spm.get_training_step(image_uri=image_uri,
                               train_s3_uri=s1.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri,
                               validation_s3_uri=s1.properties.ProcessingOutputConfig.Outputs[
                                   "validation"].S3Output.S3Uri,
                               instance_type=instance_type,
                               **h_params
                               )

    estimator = spm.get_estimator(image_uri=image_uri, instance_type=instance_type, **h_params)

    s3 = spm.get_create_model_step(image_uri=image_uri, model_data=s2.properties.ModelArtifacts.S3ModelArtifacts)

    s4 = spm.get_register_model_step(package_group_name='kwangsik',
                                     estimator=estimator,
                                     model_data=s2.properties.ModelArtifacts.S3ModelArtifacts)

    spm.register_pipeline([s1, s2, s3, s4])
    spm.start_pipeline()

    # Then
    pipeline = spm.describe_pipeline(spm.pipeline_name)
    assert pipeline


def test_start_pipeline(spm, sample_data):
    # Given
    spm.create_single_sklearn_pipeline(base_s3_uri=f'{sample_data["pipeline_name"]}/input/',
                                       code_s3_uri=f'{sample_data["pipeline_name"]}/code/preprocessing.py')

    # When
    spm.start_pipeline()


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
