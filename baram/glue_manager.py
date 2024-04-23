import os
from pathlib import Path
from typing import Optional

import awswrangler as wr
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
        self.s3_bucket_name = s3_bucket_name
        self.s3_path = f's3://{self.s3_bucket_name}'
        self.sm = S3Manager(self.s3_bucket_name)
        self.TABLE_PATH_PREFIX = table_path_prefix
        self.MAX_RESULTS = 1000
        self.GLUE_TYPE_ETL = 'glueetl'
        self.GLUE_TYPE_PYTHON_SHELL = 'pythonshell'

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
                   role_name: str,
                   security_configuration: str):
        '''

        :param name: glue job name
        :param package_name: glue jar package name
        :param role_name:  role name
        :param extra_jars: extra jar path in s3
        :param security_configuration:  security configuration
        :return:
        '''

        kwargs = self._get_create_job_kwargs(name, role_name, security_configuration)

        try:
            self.cli.create_job(
                **kwargs
            )
        except self.cli.exceptions.IdempotentParameterMismatchException as e:
            self.logger.error(str(e))

    def _get_create_job_kwargs(self,
                               job_name: str,
                               role_name: str,
                               glue_security_conf_name: str,
                               glue_job_type: str = 'glueetl',
                               worker_type: str = 'G.1X',
                               num_of_dpus=None,
                               python_module: Optional[str] = None,
                               extra_py_path: Optional[str] = None,
                               enable_iceberg: bool = True):
        '''

        :param job_name: glue job name
        :param role_name: glue role name
        :param glue_security_conf_name:
        :param glue_job_type: glue job type. 'glueetl' or 'pythonshell'.
        :param worker_type: glue worker type
        :param num_of_dpus: for pythonshell, you can allocate either 0.0625 or 1 DPU. The default is 0.0625 DPU. For glueetl, The default is 2.
        :param python_module:
        :param extra_py_path:
        :param enable_iceberg:
        :return:
        '''
        if num_of_dpus is None:
            num_of_dpus = 2 if glue_job_type == self.GLUE_TYPE_ETL else float(0.0625)

        create_job_kwargs = {
            'Name': job_name,
            'Role': role_name,
            'SecurityConfiguration': glue_security_conf_name,
            'Command': {
                'ScriptLocation': os.path.join(f's3://',
                                               self.s3_bucket_name,
                                               'scripts',
                                               f'{job_name}.py'),
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--job-language': 'python',
                '--TempDir': os.path.join(f's3://', self.s3_bucket_name, 'temp'),
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-glue-datacatalog': 'true',
                '--enable-job-insights': 'true',
                '--enable-metrics': 'true',
                '--job-bookmark-option': 'job-bookmark-enable'
            }}

        if python_module:
            create_job_kwargs['DefaultArguments'].update({'--additional-python-modules': python_module})

        default_args = {}
        if glue_job_type == self.GLUE_TYPE_ETL:
            create_job_kwargs['Command']['Name'] = self.GLUE_TYPE_ETL
            create_job_kwargs.update({
                'GlueVersion': '4.0',
                'WorkerType': worker_type,
                'NumberOfWorkers': int(num_of_dpus)})
            default_args.update({'--enable-spark-ui': 'true',
                                 '--spark-event-logs-path': os.path.join(f's3://',
                                                                         self.s3_bucket_name,
                                                                         'events/')})
            if extra_py_path:
                default_args['--extra-py-files'] = extra_py_path
            if enable_iceberg:
                default_args['--datalake-formats'] = 'iceberg'

        elif glue_job_type == self.GLUE_TYPE_PYTHON_SHELL:
            create_job_kwargs['Command']['Name'] = self.GLUE_TYPE_PYTHON_SHELL
            create_job_kwargs['Command']['PythonVersion'] = '3.9'

            if extra_py_path:
                default_args['--extra-py-files'] = extra_py_path

        create_job_kwargs['DefaultArguments'].update(default_args)

        return create_job_kwargs

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

    def get_glue_databases(self, pattern: Optional[str] = None):
        '''

        :param pattern:
        :return:
        '''

        if pattern:
            return [db['Name'] for db in wr.catalog.get_databases() if pattern in db['Name']]
        else:
            return [db['Name'] for db in wr.catalog.get_databases()]

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
