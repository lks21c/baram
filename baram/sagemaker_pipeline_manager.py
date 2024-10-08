import os.path
from typing import Optional, List

import boto3
import sagemaker
from sagemaker import TrainingInput, Model
from sagemaker.estimator import Estimator, EstimatorBase
from sagemaker.inputs import TransformInput
from sagemaker.processing import ProcessingInput, ScriptProcessor, ProcessingOutput
from sagemaker.sklearn import SKLearnProcessor
from sagemaker.transformer import Transformer
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.fail_step import FailStep
from sagemaker.workflow.model_step import ModelStep
from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
)
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession, LocalPipelineSession
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.workflow.steps import ProcessingStep, TrainingStep, TransformStep

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
        print(f'is_local_mode={is_local_mode}')
        self.role = role_arn if role_arn else sagemaker.get_execution_role()
        self.sagemaker_processor_home = '/opt/ml/processing'
        self.default_bucket = default_bucket
        self.sm = S3Manager(default_bucket)
        self.pipeline_name = pipeline_name
        self.pipeline_params = {}
        if pipeline_params:
            self.pipeline_params.update(pipeline_params)

        self.param_processing_instance_count = ParameterInteger(
            name="processing_instance_count",
            default_value=1
        )
        self.param_model_approval_status = ParameterString(
            name="model_approval_status",
            default_value="PendingManualApproval"
        )
        self.param_default_bucket = ParameterString(
            name="default_bucket",
            default_value=default_bucket
        )
        self.param_pipeline_name = ParameterString(
            name="pipeline_name",
            default_value=self.pipeline_name
        )
        self.param_base_dir = ParameterString(
            name="base_dir",
            default_value=self.sagemaker_processor_home
        )
        self.param_region = ParameterString(
            name="region",
            default_value=self.region
        )
        self.pipeline_session = PipelineSession(
            default_bucket=default_bucket) if not is_local_mode else LocalPipelineSession()

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

        step_preprocess = self.get_sklearn_step(framework_version, instance_type, base_s3_uri, code_s3_uri)
        self.register_pipeline([step_preprocess])

    def create_single_script_pipeline(self,
                                      process_name: str,
                                      ecr_image_uri: str,
                                      code_s3_uri: str,
                                      inputs: List,
                                      outputs: Optional[List] = None,
                                      instance_type: str = 'ml.t3.xlarge',
                                      property_files: Optional[List] = None):
        '''
        Create a single script pipeline

        :param process_name:
        :param ecr_image_uri:
        :param code_s3_uri:
        :param inputs:
        :param outputs:
        :param instance_type:
        :param property_files:
        :return:
        '''

        step_preprocess = self.get_script_step(process_name=process_name,
                                               ecr_image_uri=ecr_image_uri,
                                               code_s3_uri=code_s3_uri,
                                               instance_type=instance_type,
                                               inputs=inputs,
                                               outputs=outputs,
                                               property_files=property_files)
        self.register_pipeline([step_preprocess])

    def get_estimator(self, image_uri: str, instance_type: str, **hyperparameters):
        '''
        Get estimator
        :param image_uri:
        :param instance_type:
        :param hyperparameters:
        :return:
        '''
        estimator = Estimator(
            image_uri=image_uri,
            instance_type=instance_type,
            instance_count=1,
            output_path=self._get_s3_full_path(self.default_bucket, f'{self.pipeline_name}/model'),
            sagemaker_session=self.pipeline_session,
            role=self.role,
        )
        estimator.set_hyperparameters(**hyperparameters)
        return estimator

    def get_training_step(self,
                          estimator: Estimator,
                          train_s3_uri: str,
                          validation_s3_uri: str):
        '''
        Get training step

        :param estimator
        :param train_s3_uri:
        :param validation_s3_uri:
        :return:
        '''

        train_args = estimator.fit(
            inputs={
                "train": TrainingInput(
                    s3_data=train_s3_uri,
                    content_type="text/csv"
                ),
                "validation": TrainingInput(
                    s3_data=validation_s3_uri,
                    content_type="text/csv"
                )
            },
        )
        return TrainingStep(
            name=f"train_{self.pipeline_name}",
            step_args=train_args
        )

    def get_create_model_step(self,
                              image_uri: str,
                              model_data: str,
                              instance_type: str = 'ml.m5.xlarge'):
        model = Model(
            image_uri=image_uri,
            model_data=model_data,
            sagemaker_session=self.pipeline_session,
            role=self.role,
        )

        return ModelStep(
            name=f"create_model_{self.pipeline_name}",
            step_args=model.create(instance_type=instance_type),
        )

    def get_register_model_step(self,
                                package_group_name: str,
                                estimator: Optional[EstimatorBase] = None,
                                model_data=None,
                                model_metrics=None):
        '''
        Get register model step

        :param package_group_name:
        :param estimator:
        :param model_data:
        :param model_metrics:
        :return:
        '''

        return RegisterModel(
            name=f"register_model_{self.pipeline_name}",
            estimator=estimator,
            model_data=model_data,
            content_types=["text/csv"],
            response_types=["text/csv"],
            model_package_group_name=package_group_name,
            approval_status=self.param_model_approval_status,
            model_metrics=model_metrics
        )

    def register_pipeline(self, step_preprocess: List[ProcessingStep]):
        '''
        Register pipeline
        :param step_preprocess:
        :return:
        '''

        params = [
            self.param_processing_instance_count,
            self.param_model_approval_status,
            self.param_default_bucket,
            self.param_pipeline_name,
            self.param_base_dir,
            self.param_region,
            *self.pipeline_params.values()
        ]
        print(f'pipeline_params={params}')
        self.pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=params,
            steps=step_preprocess,
            sagemaker_session=self.pipeline_session
        )
        self.pipeline.upsert(role_arn=self.role)

    def get_sklearn_step(self, framework_version: str, instance_type: str, base_s3_uri: str, code_s3_uri: str):
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
            instance_count=self.param_processing_instance_count,
            base_job_name=f"sklearn-{self.pipeline_name}-process",
            sagemaker_session=self.pipeline_session,
            role=self.role,
        )

        args = self.get_processor_args()
        processor_args = sklearn_processor.run(
            inputs=[
                ProcessingInput(source=self._get_s3_full_path(self.default_bucket, base_s3_uri),
                                destination=os.path.join(self.sagemaker_processor_home, 'input')),
            ],
            code=self._get_s3_full_path(self.default_bucket, code_s3_uri),
            arguments=args if args else None
        )

        return ProcessingStep(name=f"preprocess_{self.pipeline_name}", step_args=processor_args)

    def get_script_step(self,
                        process_name: str,
                        ecr_image_uri: str,
                        code_s3_uri: str,
                        instance_type: str = 'ml.t3.xlarge',
                        inputs: Optional[List] = None,
                        outputs: Optional[List] = None,
                        property_files: Optional[List] = None):
        '''
        Get script step

        :param process_name:
        :param ecr_image_uri:
        :param instance_type:
        :param code_s3_uri:
        :param inputs:
        :param outputs:
        :param property_files:
        :return:
        '''
        script_processor = ScriptProcessor(
            image_uri=ecr_image_uri,
            role=self.role,
            instance_type=instance_type,
            instance_count=self.param_processing_instance_count,
            sagemaker_session=self.pipeline_session,
            command=['python3'],
        )

        args = self.get_processor_args()
        print(args)
        processor_args = script_processor.run(
            inputs=inputs,
            outputs=outputs if outputs else None,
            code=self._get_s3_full_path(self.default_bucket, code_s3_uri),
            arguments=args,
        )

        return ProcessingStep(name=f"script_process_{process_name}", step_args=processor_args,
                              property_files=property_files)

    def get_processor_args(self):
        args = [f'--{k}' for k, v in self.pipeline_params.items() for _ in (0, 1)]
        args[1::2] = self.pipeline_params.values()
        args += ['--default_bucket', self.param_default_bucket,
                 '--pipeline_name', self.param_pipeline_name,
                 '--base_dir', self.param_base_dir,
                 '--region', self.param_region]
        return args

    def start_pipeline(self):
        '''
        Start the pipeline

        :return:
        '''
        self.pipeline.start()

    def list_pipelines(self):
        return self.cli.list_pipelines()

    def describe_pipeline(self, pipeline_name: str):
        return self.cli.describe_pipeline(PipelineName=pipeline_name)

    def get_image_uri(self, framework: str, version: str, instance_type: str, py_version: str = 'py3'):
        '''
        Get image uri

        :param framework:
        :param version:
        :param instance_type:
        :return:
        '''
        return sagemaker.image_uris.retrieve(
            framework=framework,
            region=self.region,
            version=version,
            py_version=py_version,
            instance_type=instance_type
        )

    def get_processing_output(self,
                              output_name: str,
                              source: str,
                              destination: str):
        '''
        Get processing output

        :param output_name:
        :param source:
        :param destination:
        :return:
        '''
        return ProcessingOutput(output_name=output_name, source=source, destination=destination)

    def get_condition_step(self, conditions: List, if_steps: List, else_steps: Optional[List] = None):
        '''
        Get condition step

        :param conditions:
        :param if_steps:
        :param else_steps:
        :return:
        '''
        return ConditionStep(
            name=f"condition_step_{self.pipeline_name}",
            conditions=conditions,
            if_steps=if_steps,
            else_steps=else_steps,
        )

    def get_fail_step(self, step_name: str, error_msg: str):
        '''
        Get fail step

        :param step_name:
        :param error_msg:
        :return:
        '''
        return FailStep(
            name=step_name,
            error_message=error_msg,
        )

    def get_transformer_step(self,
                             step_name: str,
                             output_path: str,
                             model_name: str,
                             inputs: TransformInput,
                             instance_type: str = 'ml.m5.xlarge'):
        transformer = Transformer(
            model_name=model_name,
            instance_type=instance_type,
            instance_count=self.param_processing_instance_count,
            output_path=output_path,
            sagemaker_session=self.pipeline_session)

        return TransformStep(
            name=step_name,
            transformer=transformer,
            inputs=inputs
        )

    def register_model(self, image_uri: str, model_s3_uri: str, model_package_name: str):
        '''
        Register model

        :param image_uri:
        :param model_s3_uri:
        :param model_package_name:
        :return:
        '''
        model = sagemaker.model.Model(
            image_uri=image_uri,
            model_data=model_s3_uri,
            role=self.role,
            sagemaker_session=sagemaker.Session()
        )
        model.register(
            content_types=["text/csv"],
            response_types=["text/csv"],
            inference_instances=["ml.m5.large"],
            transform_instances=["ml.m5.large"],
            model_package_name=model_package_name,
            approval_status='Approved',
        )
