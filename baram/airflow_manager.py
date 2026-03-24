import boto3


class AirflowManager:
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

    def list_environments(self, max_results: int = 25) -> list:
        '''
        List MWAA environments.

        :param max_results: max number of results
        :return: list of environment names
        '''
        return self.cli.list_environments(MaxResults=max_results)['Environments']

    def create_cli_token(self, name: str) -> dict:
        '''
        Create a CLI token for an MWAA environment.

        :param name: environment name
        :return: CLI token response
        '''
        return self.cli.create_cli_token(Name=name)
