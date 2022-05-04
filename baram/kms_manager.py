import boto3


class KMSManager(object):
    def __init__(self):
        self.cli = boto3.client('kms')

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
        :param arn_or_key: True returns ARN, Flase returns key only.
        :return: arn or KMS key id.
        '''

        alias_name = f'alias/{alias_name}' if not 'alias/' in alias_name else alias_name
        for k in self.list_aliases()['Aliases']:
            if alias_name == k['AliasName']:
                return k['AliasArn'].replace(alias_name, f'key/{k["TargetKeyId"]}') if arn_or_key else k["TargetKeyId"]
