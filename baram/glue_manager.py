import os
from pathlib import Path

import boto3
import fire

from baram.iam_manager import IAMManager
from baram.log_manager import LogManager


class GlueManager(object):
    def __init__(self, s3_path):
        self.logger = LogManager.get_logger()
        self.cli = boto3.client('glue')
        self.im = IAMManager()

        self.worker_type = 'G.1X'
        self.workers_num = 2
        self.timeout = 2880
        self.max_concurrent_runs = 123
        self.max_retries = 0
        self.python_ver = '3'
        self.glue_ver = '3.0'
        self.s3_path = s3_path

        # See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        self.default_args = {
            '--job-language': 'scala',
            '--TempDir': os.path.join(self.s3_path, 'temp'),
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-glue-datacatalog': 'true',
            '--enable-job-insights': 'true',
            '--enable-metrics': 'true',
            '--enable-spark-ui': 'true',
            '--job-bookmark-option': 'job-bookmark-enable',
            '--spark-event-logs-path': os.path.join(self.s3_path, 'events/'),
            '--encryption-type': 'sse-kms'
        }

    def _get_command(self, name):
        return {
            'Name': 'glueetl',
            'ScriptLocation': os.path.join(self.s3_path, "scripts", f'{name}.scala'),
            'PythonVersion': self.python_ver
        }

    def create_job(self, name, class_name, role_name, extra_jars, security_configuration):
        self.default_args['--class'] = class_name
        self.default_args['--extra-jars'] = extra_jars

        return self.cli.create_job(
            Name=name,
            Description='',
            Role=self.im.get_role_arn(role_name),
            ExecutionProperty={
                'MaxConcurrentRuns': self.max_concurrent_runs
            },
            Command=self._get_command(name),
            DefaultArguments=self.default_args,
            MaxRetries=self.max_retries,
            Timeout=self.timeout,
            SecurityConfiguration=security_configuration,
            GlueVersion=self.glue_ver,
            NumberOfWorkers=self.workers_num,
            WorkerType=self.worker_type
        )

    def get_job(self, job_name):
        return self.cli.get_job(JobName=job_name)

    def update_job(self, name, class_name, role_name, extra_jars, security_configuration):
        self.default_args['--class'] = class_name
        self.default_args['--extra-jars'] = extra_jars

        return self.cli.update_job(
            JobName=name,
            JobUpdate={
                'Role': self.im.get_role_arn(role_name),
                'ExecutionProperty': {
                    'MaxConcurrentRuns': self.max_concurrent_runs
                },
                'Command': self._get_command(name),
                'DefaultArguments': self.default_args,
                'MaxRetries': self.max_retries,
                'Timeout': self.timeout,
                'WorkerType': self.worker_type,
                'NumberOfWorkers': self.workers_num,
                'SecurityConfiguration': security_configuration,
                'GlueVersion': self.glue_ver
            }
        )

    def delete_job(self, name):
        return self.cli.delete_job(JobName=name)

    def refresh_job(self, code_path: str, exclude_names: list):
        response = self.cli.list_jobs(MaxResults=1000)
        glue_jobs = set([f'{jn}.scala' for jn in response['JobNames']])
        git_jobs = set([f for f in os.listdir(code_path)])

        rest_in_glue = glue_jobs - git_jobs
        for f in rest_in_glue:
            name = Path(f).stem
            self.delete_job(name)
            self.logger.info(f'{name} deleted.')

        rest_in_git = git_jobs - glue_jobs
        for f in rest_in_git:
            name = Path(f).stem
            if name in exclude_names:
                continue
            self.create_job(name)
            self.logger.info(f'{name} created.')


if __name__ == '__main__':
    fire.Fire(GlueManager)
