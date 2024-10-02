import os.path
from typing import Optional, List

import boto3
import sagemaker
import sagemaker.session
from sagemaker.processing import ProcessingInput
from sagemaker.sklearn import SKLearnProcessor
from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession, LocalPipelineSession
from sagemaker.workflow.steps import ProcessingStep

from baram.s3_manager import S3Manager


class SagemakerPipelineManager(object):
    def __init__(self,
                 default_bucket: str,
                 pipeline_name: str,
                 role_arn: Optional[str] = None,
                 pipeline_params: Optional[dict] = {},
                 is_local_mode: bool = False):
        '''

        :param default_bucket:
        :param pipeline_name:
        '''

        self.cli = boto3.client('sagemaker')
        self.region = boto3.Session().region_name
        self.sagemaker_session = sagemaker.session.Session() if not is_local_mode else LocalPipelineSession()
        self.role = role_arn if role_arn else sagemaker.get_execution_role()
        self.sagemaker_processor_home = '/opt/ml/processing'

        self.default_bucket = default_bucket
        self.sm = S3Manager(default_bucket)
        self.pipeline_name = pipeline_name
        self.pipeline_params = {'default_bucket': default_bucket, 'pipeline_name': pipeline_name,
                                'base_dir': self.sagemaker_processor_home}
        if pipeline_params:
            self.pipeline_params.update(pipeline_params)

        self.pipeline_session = PipelineSession(default_bucket=default_bucket)

        self.processing_instance_count = ParameterInteger(
            name="ProcessingInstanceCount",
            default_value=1
        )
        self.model_approval_status = ParameterString(
            name="ModelApprovalStatus",
            default_value="PendingManualApproval"
        )

    def upload_local_files(self, local_dir: str):
        '''

        :param local_dir:
        :return:
        '''
        target_dir = f'{self.pipeline_name}/{local_dir}/'
        self.sm.upload_dir(local_dir, target_dir)
        print(f"Uploaded to {self._get_s3_web_url(self.default_bucket, target_dir)}")
        self.base_uri = self._get_s3_full_path(self.default_bucket, target_dir)

    def _get_s3_full_path(self, s3_bucket_name: str, path: str):
        '''
        Get s3 full path.

        :param s3_bucket_name: bucket name
        :param path: path
        :return:
        '''
        return f's3://{s3_bucket_name}/{path}'

    def _get_s3_web_url(self, s3_bucket_name, path: str, region: str = 'ap-northeast-2'):
        '''
        get s3 web url

        :param s3_bucket_name: s3 bucket name
        :param path: s3 path
        :param region: s3 region
        :return:
        '''
        return f'https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket_name}?region={region}&prefix={path}'

    def create_single_sklearn_pipeline(self,
                                       framework_version: str = '1.2-1',
                                       instance_type: str = 'ml.t3.xlarge',
                                       base_s3_uri: Optional[str] = None,
                                       code_s3_uri: Optional[str] = None):
        '''
        Create a single sklearn pipeline
        :param framework_version: sklearn framework version
        :param instance_type:
        :param base_s3_uri:
        :param code_s3_uri:
        :return:
        '''

        step_preprocess = self.get_preprocess_step(framework_version, instance_type, base_s3_uri, code_s3_uri)
        self.register_pipeline(step_preprocess)

    def register_pipeline(self, step_preprocess: List[ProcessingStep]):
        '''
        Register pipeline
        :param step_preprocess:
        :return:
        '''

        params = [
            self.processing_instance_count,
            self.model_approval_status,
            #*self.pipeline_params.values() # TODO: pipeline params 제대로 등록해야함
        ]
        print(f'pipeline_params={params}')
        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=params,
            steps=[step_preprocess],
        )
        self.pipeline.upsert(role_arn=self.role)

    def get_preprocess_step(self, framework_version: str, instance_type: str, base_s3_uri: str, code_s3_uri: str):
        '''
        Get preprocess step
        :param framework_version: sklearn framework version
        :param instance_type:
        :param base_s3_uri:
        :param code_s3_uri:
        :return:
        '''

        sklearn_processor = SKLearnProcessor(
            framework_version=framework_version,
            instance_type=instance_type,
            instance_count=self.processing_instance_count,
            base_job_name=f"sklearn-{self.pipeline_name}-process",
            sagemaker_session=self.pipeline_session,
            role=self.role,
        )

        args = [f'--{k}' for k, v in self.pipeline_params.items() for _ in (0, 1)]
        args[1::2] = self.pipeline_params.values()

        processor_args = sklearn_processor.run(
            inputs=[
                ProcessingInput(source=self._get_s3_full_path(self.default_bucket, base_s3_uri),
                                destination=os.path.join(self.sagemaker_processor_home, 'input')),
            ],
            code=self._get_s3_full_path(self.default_bucket, code_s3_uri),
            arguments=args if args else None
        )

        return ProcessingStep(name=f"{self.pipeline_name}Process", step_args=processor_args)

    def start_pipeline(self):
        '''
        Start the pipeline

        :return:
        '''
        self.pipeline.start()

    def list_pipelines(self):
        return self.cli.list_pipelines()

    def describe_pipeline(self, pipeline_name:str):
        return self.cli.describe_pipeline(PipelineName=pipeline_name)