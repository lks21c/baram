import os
from pathlib import Path
from pprint import pprint
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
        self.GLUE_TYPE_ETL_PYTHON_VERSION = '3'
        self.GLUE_TYPE_PYTHON_SHELL = 'pythonshell'
        self.GLUE_TYPE_PYTHON_SHELL_PYTHON_VERSION = '3.9'

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
                   job_name: str,
                   role_name: str,
                   glue_security_conf_name: str,
                   glue_job_type: str = 'glueetl',
                   worker_type: str = 'G.1X',
                   num_of_dpus=None,
                   script_location: Optional[str] = None,
                   python_library_path: Optional[str] = None,
                   job_params: Optional[dict] = None,
                   python_module: Optional[str] = None,
                   enable_iceberg: bool = True):
        '''

         :param job_name: glue job name
         :param role_name: glue role name
         :param glue_security_conf_name:
         :param glue_job_type: glue job type. 'glueetl' or 'pythonshell'.
         :param worker_type: glue worker type
         :param num_of_dpus: for pythonshell, you can allocate either 0.0625 or 1 DPU. The default is 0.0625 DPU. For glueetl, The default is 2.
         :param script_location:
         :param python_library_path:
         :param job_params:
         :param python_module:
         :param enable_iceberg:
         :return:
         '''

        kwargs = self._get_create_job_kwargs(job_name=job_name,
                                             role_name=role_name,
                                             glue_security_conf_name=glue_security_conf_name,
                                             glue_job_type=glue_job_type,
                                             worker_type=worker_type,
                                             num_of_dpus=num_of_dpus,
                                             script_location=script_location,
                                             python_library_path=python_library_path,
                                             job_params=job_params,
                                             python_module=python_module,
                                             enable_iceberg=enable_iceberg)

        try:
            self.cli.create_job(**kwargs)
        except self.cli.exceptions.IdempotentParameterMismatchException as e:
            self.logger.error(str(e))

    def _get_create_job_kwargs(self,
                               job_name: str,
                               role_name: str,
                               glue_security_conf_name: str,
                               glue_job_type: str = 'glueetl',
                               worker_type: str = 'G.1X',
                               num_of_dpus=None,
                               script_location: Optional[str] = None,
                               python_library_path: Optional[str] = None,
                               job_params: Optional[dict] = None,
                               python_module: Optional[str] = None,
                               enable_iceberg: bool = True):
        '''

        :param job_name: glue job name
        :param role_name: glue role name
        :param glue_security_conf_name:
        :param glue_job_type: glue job type. 'glueetl' or 'pythonshell'.
        :param worker_type: glue worker type
        :param num_of_dpus: for pythonshell, you can allocate either 0.0625 or 1 DPU. The default is 0.0625 DPU. For glueetl, The default is 2.
        :param script_location:
        :param python_library_path:
        :param job_params:
        :param python_module:
        :param enable_iceberg:
        :return:
        '''
        if num_of_dpus is None:
            num_of_dpus = 2 if glue_job_type == self.GLUE_TYPE_ETL else float(0.0625)

        if script_location is None:
            script_location = os.path.join(f's3://',
                                           self.s3_bucket_name,
                                           'scripts',
                                           f'{job_name}.py')

        create_job_kwargs = {
            'Name': job_name,
            'Role': role_name,
            'SecurityConfiguration': glue_security_conf_name,
            'Command': {
                'ScriptLocation': script_location,
                'PythonVersion': self.GLUE_TYPE_ETL_PYTHON_VERSION
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
            if python_library_path:
                default_args['--extra-py-files'] = python_library_path
            if enable_iceberg:
                default_args['--datalake-formats'] = 'iceberg'

        elif glue_job_type == self.GLUE_TYPE_PYTHON_SHELL:
            create_job_kwargs['Command']['Name'] = self.GLUE_TYPE_PYTHON_SHELL
            create_job_kwargs['Command']['PythonVersion'] = self.GLUE_TYPE_PYTHON_SHELL_PYTHON_VERSION

            if python_library_path:
                default_args['--extra-py-files'] = python_library_path

        create_job_kwargs['DefaultArguments'].update(default_args)
        if job_params:
            create_job_kwargs['DefaultArguments'].update(job_params)

        return create_job_kwargs

    def get_job(self, job_name: str):
        '''

        :param job_name: glue job name.
        :return: glue job
        '''
        try:
            return self.cli.get_job(JobName=job_name)
        except self.cli.exceptions.EntityNotFoundException:
            return None

    # TODO: add more parameters
    def update_job(self,
                   job_name: str,
                   glue_job_type: str = 'glueetl',
                   worker_type: str = 'G.1X',
                   num_of_dpus=None,
                   python_library_path: Optional[str] = None,
                   python_module: Optional[str] = None,
                   enable_iceberg: bool = True):

        '''
        Updates an existing Glue job with the provided parameters.

        :param job_name: The name of the Glue job to update.
        :param glue_job_type: The type of Glue job. Default is 'glueetl'. Can be 'glueetl' or 'pythonshell'.
        :param worker_type: The type of Glue worker. Default is 'G.1X'.
        :param num_of_dpus: The number of DPUs for the Glue job. Default is None.
        :param python_library_path: The S3 path to the Python library .zip file. Default is None.
        :param python_module: The Python module to use. Default is None.
        :param enable_iceberg: Whether to enable Iceberg for the Glue job. Default is True.
        :return: The response from the `update_job` API call.
        '''

        prev_job = self.get_job(job_name)
        print(f'prev_job: {prev_job}')

        prev_job['Job']['Command']['Name'] = glue_job_type

        # Remove unnecessary keys
        prev_job['Job'].pop('Name', None)
        prev_job['Job'].pop('CreatedOn', None)
        prev_job['Job'].pop('LastModifiedOn', None)
        prev_job['Job'].pop('AllocatedCapacity', None)
        prev_job['Job'].pop('MaxCapacity', None)

        if num_of_dpus is None:
            num_of_dpus = 2 if glue_job_type == self.GLUE_TYPE_ETL else float(0.0625)

        if glue_job_type == self.GLUE_TYPE_PYTHON_SHELL:
            prev_job['Job'].pop('WorkerType', None)
            prev_job['Job'].pop('NumberOfWorkers', None)
            prev_job['Job']['MaxCapacity'] = float(num_of_dpus) if num_of_dpus else 1
            prev_job['Job']['Command']['PythonVersion'] = self.GLUE_TYPE_PYTHON_SHELL_PYTHON_VERSION
            prev_job['Job']['DefaultArguments'].pop('--enable-spark-ui', None)
        else:
            prev_job['Job']['WorkerType'] = worker_type
            prev_job['Job']['NumberOfWorkers'] = num_of_dpus
            prev_job['Job']['Command']['PythonVersion'] = self.GLUE_TYPE_ETL_PYTHON_VERSION

        if python_library_path:
            prev_job['Job']['DefaultArguments']['--extra-py-files'] = python_library_path

        if python_module:
            prev_job['Job']['DefaultArguments']['--additional-python-modules'] = python_module

        if enable_iceberg:
            prev_job['Job']['DefaultArguments']['--datalake-formats'] = 'iceberg'
        else:
            prev_job['Job']['DefaultArguments'].pop('--datalake-formats', None)

        return self.cli.update_job(
            JobName=job_name,
            JobUpdate=prev_job['Job']
        )

    def rename_job(self, old_name: str, new_name: str):
        '''

        :param old_name: old job name
        :param new_name: new job name
        :return:
        '''
        # Get the old job
        old_job = self.get_job(old_name)
        old_job_params = old_job['Job']

        if old_job_params['Command']['Name'] == self.GLUE_TYPE_PYTHON_SHELL:
            dpu = old_job_params['MaxCapacity']
        else:
            dpu = old_job_params['AllocatedCapacity']
        pprint(old_job_params)

        # Create a new job with the new name and the same parameters as the old job
        self.create_job(job_name=new_name,
                        role_name=old_job_params['Role'],
                        glue_security_conf_name=old_job_params['SecurityConfiguration'],
                        glue_job_type=old_job_params['Command']['Name'],
                        num_of_dpus=dpu,
                        worker_type=old_job_params['WorkerType'],
                        script_location=old_job_params['Command']['ScriptLocation'],
                        python_library_path=old_job_params['DefaultArguments']['--additional-python-modules'],
                        python_module=old_job_params['DefaultArguments']['--extra-py-files'],
                        enable_iceberg='--datalake-formats' in old_job_params['DefaultArguments'])

        # Delete the old job
        self.delete_job(old_name)

    def delete_job(self, name: str):
        '''

        :param name: job name
        :return:
        '''
        return self.cli.delete_job(JobName=name)

    def get_table(self, db_name: str, table_name: str):
        '''

        :param db_name: database name
        :param table_name: table name
        :return:
        '''
        return self.cli.get_table(
            DatabaseName=db_name,
            Name=table_name
        )['Table']

    def get_tables(self, db_name: str):
        '''

        :param db_name:
        :return:
        '''
        return self.cli.get_tables(
            DatabaseName=db_name
        )['TableList']

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

        # TODO: compare between real job and s3
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
