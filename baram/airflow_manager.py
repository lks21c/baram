import boto3


class AirflowManager(object):
    def __init__(self):
        self.cli = boto3.client('mwaa')

    def get_environment(self, name: str):
        '''
        Describes an Amazon Managed Workflows for Apache Airflow (MWAA) environment.

        :param name: environment name
        :return: Environment response
        '''
        return self.cli.get_environment(Name=name)['Environment']

    def update_environment(self, name: str):
        '''
        Updates an Amazon Managed Workflows for Apache Airflow (MWAA) environment.

        :param name: environment name
        :return:
        '''
        self.cli.update_environment(Name=name)
