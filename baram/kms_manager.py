from typing import Optional

import boto3
from botocore.config import Config


class KMSManager:
    def __init__(self, region: Optional[str] = 'ap-northeast-2'):
        config = Config(region_name=region, signature_version='v4')
        self.cli = boto3.client('kms', config=config)

    def list_keys(self):
        '''
        Gets a list of all KMS keys in the caller's Amazon Web Services account and Region.

        :return: KMS keys.
        '''
        return self.cli.list_keys()

    def list_aliases(self):
        '''
        Gets a list of aliases in the caller's Amazon Web Services account and region.

        :return: KMS aliases.
        '''
        return self.cli.list_aliases()

    def get_kms_arn(self, alias_name: str, arn_or_key: bool = True):
        '''
        Retrieve kms arn by alias.

        :param alias_name: alias name.
        :param arn_or_key: True returns ARN, False returns key only.
        :return: arn or KMS key id.
        '''

        alias_name = f'alias/{alias_name}' if not alias_name.startswith('alias/') else alias_name
        for k in self.list_aliases()['Aliases']:
            if alias_name == k['AliasName']:
                return k['AliasArn'].replace(alias_name, f'key/{k["TargetKeyId"]}') if arn_or_key else k["TargetKeyId"]

    def describe_key(self, key_id: str) -> dict:
        '''
        Describe a KMS key.

        :param key_id: key id or ARN
        :return: key metadata
        '''
        return self.cli.describe_key(KeyId=key_id)['KeyMetadata']

    def create_alias(self, alias_name: str, target_key_id: str):
        '''
        Create an alias for a KMS key.

        :param alias_name: alias name
        :param target_key_id: target key id
        :return:
        '''
        alias_name = f'alias/{alias_name}' if not alias_name.startswith('alias/') else alias_name
        return self.cli.create_alias(AliasName=alias_name, TargetKeyId=target_key_id)

    def delete_alias(self, alias_name: str):
        '''
        Delete a KMS alias.

        :param alias_name: alias name
        :return:
        '''
        alias_name = f'alias/{alias_name}' if not alias_name.startswith('alias/') else alias_name
        return self.cli.delete_alias(AliasName=alias_name)
