import os
from pathlib import Path

import boto3
import fire

from baram.iam_manager import IAMManager
from baram.log_manager import LogManager
from baram.s3_manager import S3Manager


class GlueManager(object):
    def __init__(self, s3_bucket_name: str, table_path_prefix='table'):
        '''

        :param s3_bucket_name: s3 bucket name where Glue uses as default.
        '''

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
        self.s3_path = f's3://{s3_bucket_name}'
        self.sm = S3Manager(s3_bucket_name)
        self.TABLE_PATH_PREFIX = table_path_prefix
        self.MAX_RESULTS = 1000

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

    def start_job_run(self, name: str):
        '''

        :param name: job name
        :return:
        '''
        return self.cli.start_job_run(
            JobName=name
        )

    def _get_command(self, name: str):
        '''

        :param name: get command object when you create or update glue job.
        :return:
        '''

        return {
            'Name': 'glueetl',
            'ScriptLocation': os.path.join(self.s3_path, "scripts", f'{name}.scala'),
            'PythonVersion': self.python_ver
        }

    def create_job(self,
                   name: str,
                   package_name: str,
                   role_name: str,
                   extra_jars: str,
                   security_configuration: str):
        '''

        :param name: glue job name
        :param package_name: glue jar package name
        :param role_name:  role name
        :param extra_jars: extra jar path in s3
        :param security_configuration:  security configuration
        :return:
        '''

        self.default_args['--class'] = f'{package_name}.{name}'
        self.default_args['--extra-jars'] = extra_jars

        try:
            self.cli.create_job(
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
        except self.cli.exceptions.IdempotentParameterMismatchException as e:
            self.logger.error(str(e))

    def get_job(self, job_name: str):
        '''

        :param job_name: glue job name.
        :return: glue job
        '''
        return self.cli.get_job(JobName=job_name)

    def update_job(self,
                   name: str,
                   package_name: str,
                   role_name: str,
                   extra_jars: str,
                   security_configuration: str):
        '''

        :param name: job name
        :param package_name: glue jar package name
        :param role_name:  role name
        :param extra_jars: extra jar path in s3
        :param security_configuration:  security configuration
        :return:
        '''

        self.default_args['--class'] = f'{package_name}.{name}'
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

    def delete_job(self, name: str):
        '''

        :param name: job name
        :return:
        '''
        return self.cli.delete_job(JobName=name)

    def delete_table(self, db_name: str, table_name: str, include_s3: bool = False):
        '''

        :param db_name: database name
        :param table_name: job name
        :param include_s3: delete table including s3 or not
        :return:
        '''
        try:
            self.cli.delete_table(
                DatabaseName=db_name,
                Name=table_name
            )
        except Exception as e:
            pass
        finally:
            if include_s3:
                print(f'delete {os.path.join(self.TABLE_PATH_PREFIX, db_name, table_name)}')
                self.sm.delete_dir(os.path.join(self.TABLE_PATH_PREFIX, db_name, table_name))

    def get_table(self, db_name: str, table_name: str):
        '''

        :param db_name: database name
        :param table_name: table name
        :return:
        '''
        return self.cli.get_table(
            DatabaseName=db_name,
            Name=table_name
        )

    def list_job_names(self,
                       max_results: int = 50,
                       name_filter: str = ''):
        '''
        List glue jobs

        :param max_results:
        :param name_filter:
        :return:
        '''
        max_results = max_results if max_results else self.MAX_RESULTS
        jobs = self.cli.list_jobs(MaxResults=max_results)['JobNames']

        return [job for job in jobs if name_filter in jobs]

    def refresh_job(self,
                    code_path: str,
                    exclude_names: list,
                    package_name: str,
                    role_name: str,
                    extra_jars: str,
                    security_configuration: str):
        '''

        :param code_path: code path
        :param exclude_names: job names to be excluded
        :param package_name: glue jar package name
        :param role_name: glue role name
        :param extra_jars: extra jar path in s3
        :param security_configuration: security configuraton
        :return:
        '''

        glue_jobs = set([f'{jn}.scala' for jn in self.list_job_names()])
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
            self.create_job(name, package_name, role_name, extra_jars, security_configuration)
            self.logger.info(f'{name} created.')

    def summary(self):
        jobs = self.cli.list_jobs()
        if 'JobNames' in jobs:
            for j in jobs['JobNames']:
                r = self.cli.get_job_runs(JobName=j)
                if 'JobRuns' in r and len(r['JobRuns']) > 0:
                    last_run = r['JobRuns'][0]
                    last_run['StartedOn']

                    duration = int((last_run['CompletedOn'] - last_run['StartedOn']).total_seconds())
                    print(
                        f"{last_run['JobName']}\t{last_run['AllocatedCapacity']}\t{last_run['StartedOn']}\t{last_run['CompletedOn']}\t{duration}")


if __name__ == '__main__':
    fire.Fire(GlueManager)
