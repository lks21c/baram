import json
import base64
from typing import Optional, Tuple

import boto3
import requests


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

    def get_mwaa_cli_token(self, mwaa_env_name: str):
        '''
        Get AWS CLI token of MWAA

        :param mwaa_env_name: name of MWAA environment
        :return:
        '''
        return self.cli.create_cli_token(Name=mwaa_env_name)

    def get_mwaa_auth_token(self, mwaa_env_name: str):
        '''
        Get authentication token of MWAA

        :param mwaa_env_name: name of MWAA environment
        :return:
        '''
        return 'Bearer ' + self.cli.create_cli_token(Name=mwaa_env_name)['CliToken']

    def get_mwaa_server_url(self, mwaa_env_name: str):
        '''
        Get url of specific MWAA webserver

        :param mwaa_env_name: name of MWAA environment
        :return:
        '''
        return f'https://{self.get_mwaa_cli_token(mwaa_env_name=mwaa_env_name)}/aws_mwaa/cli'

    def request_mwaa_command(self,
                             mwaa_env_name: str,
                             mwaa_cli_command: str):
        '''
        Request command for triggering MWAA DAG

        :param mwaa_env_name: name of MWAA environment for request
        :param mwaa_cli_command: data for request, i.e. cli command (ex: dags trigger)
        :return:
        '''

        mwaa_response = requests.post(
            url=self.get_mwaa_server_url(mwaa_env_name=mwaa_env_name),
            headers={
                'Authorization': self.get_mwaa_auth_token(mwaa_env_name=mwaa_env_name),
                'Content-Type': 'text/plain'
            },
            data=mwaa_cli_command
        )

        mwaa_std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
        mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')

        assert mwaa_response.status_code == 200

        print(f'mwaa_std_err_message={mwaa_std_err_message}')
        print(f'mwaa_std_out_message={mwaa_std_out_message}')

    def trigger_mwaa_dag_with_rest(self,
                                   mwaa_env_name: str,
                                   mwaa_dag_id: str,
                                   params: dict = {}):
        '''
        Trigger MWAA DAG with REST API

        :param mwaa_env_name: name of MWAA environment
        :param mwaa_dag_id: ID of MWAA DAG to trigger
        :param params: dag_run.conf to inject
        :return:
        '''

        raw_data = "{0} {1} -c '{2}'".format('dags trigger', mwaa_dag_id, json.dumps(params))

        self.request_mwaa_command(mwaa_env_name=mwaa_env_name,
                                  mwaa_cli_command=raw_data)

    def trigger_dag_with_rest(self,
                              dag_id: str,
                              server_ip: str,
                              server_port: str,
                              auth: Optional[Tuple[str, str]] = None,
                              params: dict = {}):
        '''
        Trigger DAG with REST API

        :param dag_id: ID of dag to trigger
        :param server_ip: IP of airflow server
        :param server_port: Port of airflow server
        :param auth: pair of username and password of airflow server
        :param params: dag_run.conf to inject
        :return:
        '''

        data = {'conf': json.dumps(params)}
        headers = {'accept': 'application/json', 'Content-Type': 'application/json'}

        response = requests.post(
            url=f'http://{server_ip}:{server_port}/api/vi/dags/{dag_id}/dagRuns',
            auth=auth,
            headers=headers,
            data=data
        )

        assert response.status_code == 200

        response_json = response.json()
        for key in response_json.keys():
            print(f'{key}: {response_json[key]}')
