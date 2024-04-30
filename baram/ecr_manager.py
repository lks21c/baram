import boto3


class ECRManager(object):
    def __init__(self):
        self.cli = boto3.client('ecr')

    def describe_repositories(self,
                              max_results=100,
                              **kwargs):
        response = self.cli.describe_repositories(maxResults=max_results,
                                                  **kwargs)
        return response

    def describe_images(self,
                        repo_name: str,
                        max_results: int = 100,
                        **kwargs):
        '''

        :param repo_name: repo name
        :param max_results: default 100
        :return:
        '''
        response = self.cli.describe_images(repositoryName=repo_name,
                                            maxResults=max_results,
                                            **kwargs)
        return response['imageDetails']

    def list_images(self, repo_name: str,
                    max_results: int = 100):
        '''

        :param repo_name: repo name
        :param max_results: default 100
        :return:
        '''
        response = self.cli.list_images(repositoryName=repo_name,
                                        maxResults=max_results)
        return response['imageIds']
