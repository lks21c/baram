from logging import Logger

import fire
import boto3
from botocore.client import BaseClient

from baram.log_manager import LogManager


class IAMManager(object):
    def __init__(self) -> None:
        self.cli: BaseClient = boto3.client('iam')
        self.logger: Logger = LogManager.get_logger()

    def get_role(self, role_name: str) -> list:
        '''
        Retrieves information about the specified role.

        :param role_name: role name
        :return:
        '''
        return self.cli.get_role(RoleName=role_name)['Role']

    def get_role_arn(self, role_name: str) -> list:
        '''
        Retrieves role ARN.

        :param role_name: role name
        :return:
        '''
        return self.get_role(role_name)['Arn']

    def list_role_policies(self, role_name: str) -> list:
        '''
        Lists the names of the inline policies that are embedded in the specified IAM role.

        :param role_name: role name
        :return:
        '''
        return self.cli.list_attached_role_policies(RoleName=role_name)['AttachedPolicies']

    def attach_group_policy(self, group_name: str, policy_arn: str) -> None:
        '''
        Attaches the specified managed policy to the specified IAM group.

        :param group_name: group name
        :param policy_arn: policy arn
        :return:
        '''
        return self.cli.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)

    def detach_group_policy(self, group_name: str, policy_arn: str) -> None:
        '''
        Removes the specified managed policy from the specified IAM group.

        :param group_name: group name
        :param policy_arn: policy arn
        :return:
        '''
        return self.cli.detach_group_policy(GroupName=group_name, PolicyArn=policy_arn)

    def list_group_policies(self, user_group_name: str, max_items: int = 100) -> dict:
        '''
        Lists the names of the inline policies that are embedded in the specified IAM group.

        :param user_group_name: user group name
        :param max_items: max items, default=100
        :return:
        '''
        return self.cli.list_group_policies(GroupName=user_group_name, MaxItems=max_items)

    def list_policies(self, scope: str = 'All', max_result: int = 1000) -> list:
        """
        Lists policies in IAM
        :param scope: 'All' for all policies, 'Local' for customer managed policies, 'AWS' for AWS managed policies
        :param max_result: max number of results (max=1000)
        :return:
        """
        policies: dict = self.cli.list_policies(Scope=scope, MaxItems=max_result)
        result: list = policies['Policies']
        while 'Marker' in policies:
            policies: dict = self.cli.list_policies(Scope=scope, MaxItems=max_result, Marker=policies['Marker'])
            result += policies['Policies']
        return result

    def list_redundant_policies(self, scope: str = 'Local') -> list:
        """
        Lists redundant policies that are not attached to any IAM user, group, or role
        :param scope: 'All' for all policies, 'Local' for customer managed policies, 'AWS' for AWS managed policies
        :return:
        """
        return [i for i in self.list_policies(scope=scope) if i['AttachmentCount'] == 0]
