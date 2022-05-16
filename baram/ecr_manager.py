import boto3


class ECRManager(object):
    def __init__(self):
        self.cli = boto3.client('ecr')

    def describe_images(self, name: str, max_result: int = 100):
        '''

        :param name: repo name
        :param max_result: default 100
        :return:
        '''
        response = self.cli.describe_images(
            repositoryName=name,
            maxResults=max_result,
        )
        return response['imageDetails']

    def list_images(self, name: str, max_result: int = 100):
        '''

        :param name: repo name
        :param max_result: default 100
        :return:
        '''
        response = self.cli.list_images(
            repositoryName=name,
            maxResults=max_result
        )
        return response['imageIds']
