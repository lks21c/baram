import base64

import boto3


class ECRManager:
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

    def create_repository(self, repo_name: str, image_tag_mutability: str = 'MUTABLE',
                          scan_on_push: bool = True) -> dict:
        '''
        Create an ECR repository.

        :param repo_name: repository name
        :param image_tag_mutability: MUTABLE or IMMUTABLE
        :param scan_on_push: enable image scanning on push
        :return: repository info
        '''
        return self.cli.create_repository(
            repositoryName=repo_name,
            imageTagMutability=image_tag_mutability,
            imageScanningConfiguration={'scanOnPush': scan_on_push})['repository']

    def delete_repository(self, repo_name: str, force: bool = False):
        '''
        Delete an ECR repository.

        :param repo_name: repository name
        :param force: force delete even if images exist
        :return:
        '''
        return self.cli.delete_repository(repositoryName=repo_name, force=force)

    def delete_images(self, repo_name: str, image_ids: list):
        '''
        Batch delete images from an ECR repository.

        :param repo_name: repository name
        :param image_ids: list of {'imageTag': 'x'} or {'imageDigest': 'x'}
        :return:
        '''
        return self.cli.batch_delete_image(repositoryName=repo_name, imageIds=image_ids)

    def get_login_password(self) -> str:
        '''
        Get ECR login password (base64-decoded authorization token).

        :return: password string
        '''
        response = self.cli.get_authorization_token()
        token = response['authorizationData'][0]['authorizationToken']
        return base64.b64decode(token).decode('utf-8').split(':')[1]
