import time
from typing import Optional

import boto3

from baram.log_manager import LogManager


class SagemakerManager(object):
    def __init__(self, domain_name: str):
        self.cli = boto3.client('sagemaker')
        self.domain_id = self.get_domain_id(domain_name=domain_name)
        self.logger = LogManager.get_logger('SagemakerManager')

    def get_domain_id(self,
                      domain_name: str):
        """
        :param domain_name: sagemaker domain's name
        :return:
        """
        response = self.list_domains()
        for i in response:
            if domain_name == i['DomainName']:
                return i['DomainId']

    def list_domains(self):
        return self.cli.list_domains()['Domains']

    def describe_domain(self,
                        domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id

        try:
            return self.cli.describe_domain(DomainId=domain_id)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'domain {domain_id} does not exist')
            return None

    def create_domain(self,
                      domain_name: str,
                      auth_mode: str,
                      execution_role_arn: str,
                      sg_groups: list,
                      subnet_ids: list,
                      vpc_id: str,
                      app_network_access_type: str,
                      efs_kms_id: str,
                      **kwargs):
        """

        :param domain_name: sagemaker domain name
        :param auth_mode: SSO' or 'IAM'
        :param execution_role_arn: execution IAM role's arn
        :param sg_groups: list of security groups
        :param subnet_ids: list of subnet ids
        :param vpc_id: vpc id in which the domain uses for communication
        :param app_network_access_type: 'PublicInternetOnly' or 'VpcOnly'
        :param efs_kms_id: kms key id of related efs
        :param kwargs: additional keyword arguments
        :return:
        """
        try:
            self.cli.create_domain(DomainName=domain_name,
                                   AuthMode=auth_mode,
                                   DefaultUserSettings={
                                       'ExecutionRole': execution_role_arn,
                                       'SecurityGroups': sg_groups},
                                   SubnetIds=subnet_ids,
                                   VpcId=vpc_id,
                                   AppNetworkAccessType=app_network_access_type,
                                   KmsKeyId=efs_kms_id,
                                   DomainSettings={
                                       'SecurityGroupIds': sg_groups},
                                   **kwargs)
            domain_id = self.get_domain_id(domain_name)
            while self.describe_domain(domain_id=domain_id)['Status'] == 'Pending':
                time.sleep(10)
            self.logger.info(f'{domain_name} created')
        except self.cli.exceptions.ResourceInUse:
            self.logger.info(f'{domain_name} already exists')
            return None

    def delete_domain(self,
                      domain_id: Optional[str] = None,
                      delete_user_profiles: Optional[bool] = True,
                      retention_policy: Optional[str] = 'Delete'):
        domain_id = domain_id if domain_id else self.domain_id

        if delete_user_profiles:
            user_profiles = self.list_user_profiles(domain_id=domain_id)
            for i in user_profiles:
                self.delete_user_profile(user_profile_name=i['UserProfileName'], domain_id=domain_id)
        domain_name = self.describe_domain(domain_id=domain_id)['DomainName']

        try:
            self.cli.delete_domain(DomainId=domain_id, RetentionPolicy={'HomeEfsFileSystem': retention_policy})
            while domain_id in [x['DomainId'] for x in self.list_domains()]:
                time.sleep(10)
            self.logger.info(f'{domain_name} deleted')
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{domain_name} does not exist')
            return None

    def list_user_profiles(self,
                           domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        return self.cli.list_user_profiles(DomainIdEquals=domain_id)['UserProfiles']

    def describe_user_profile(self,
                              user_profile_name: str,
                              domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            return self.cli.describe_user_profile(DomainId=domain_id,
                                                  UserProfileName=user_profile_name)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{user_profile_name} does not exist')
            return None

    def create_user_profile(self,
                            user_profile_name: str,
                            execution_role: str,
                            domain_id: Optional[str] = None,
                            **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            self.cli.create_user_profile(DomainId=domain_id,
                                         UserProfileName=user_profile_name,
                                         UserSettings={
                                             'ExecutionRole': execution_role},
                                         **kwargs)
            while self.describe_user_profile(user_profile_name=user_profile_name)['Status'] == 'Pending':
                time.sleep(5)
            self.logger.info(f'{user_profile_name} created')
        except self.cli.exceptions.ResourceInUse:
            self.logger.info(f'{user_profile_name} already exists.')
            return None

    def delete_user_profile(self,
                            user_profile_name: str,
                            domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            self.describe_user_profile(user_profile_name=user_profile_name,
                                       domain_id=domain_id)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'user {user_profile_name} does not exist.')
            return None

        self.logger.info(f'list apps from {user_profile_name}')
        apps = self.list_apps(domain_id=domain_id,
                              UserProfileNameEquals=user_profile_name)
        for app in apps:
            try:
                response = self.describe_app(user_profile_name=user_profile_name,
                                             app_name=app['AppName'],
                                             app_type=app['AppType'],
                                             domain_id=domain_id)
                if response['Status'] != 'Deleted' and response['Status'] != 'Deleting':
                    self.delete_app(user_profile_name=user_profile_name,
                                    app_name=app['AppName'],
                                    app_type=app['AppType'],
                                    domain_id=domain_id)
            except self.cli.exceptions.ResourceNotFound:
                pass
            except self.cli.exceptions.ResourceInUse as e:
                self.logger.info(e)
                return
        self.logger.info(f'deleting {len(apps)} apps.')
        delete_cnt = 0
        elapsed_secs = 0
        while delete_cnt < len(apps):
            delete_cnt = 0
            for app in apps:
                response = self.describe_app(user_profile_name=user_profile_name,
                                             app_name=app['AppName'],
                                             app_type=app['AppType'],
                                             domain_id=domain_id)
                self.logger.info(f'status = {response["Status"]}')
                if response['Status'] == 'Deleted' or response['Status'] == 'Failed':
                    delete_cnt += 1
            time.sleep(5)
            elapsed_secs += 5
            self.logger.info(f'wait 5 seconds. delete_cnt={delete_cnt}, elapsed_secs={elapsed_secs}')
        self.cli.delete_user_profile(DomainId=domain_id, UserProfileName=user_profile_name)
        while user_profile_name in [x['UserProfileName'] for x in self.list_user_profiles()]:
            time.sleep(5)
        self.logger.info(f'{user_profile_name} deleted')

    def recreate_all_user_profiles(self,
                                   domain_id: Optional[str] = None,
                                   **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            user_profiles = [self.describe_user_profile(user_profile_name=x['UserProfileName'], domain_id=domain_id)
                             for x in self.list_user_profiles(domain_id=domain_id)]
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'There are no user profiles to recreated in {domain_id}')
            return None

        self.logger.info(f"User profiles to recreate: {[x['UserProfileName'] for x in user_profiles]}")

        for i in user_profiles:
            self.delete_user_profile(user_profile_name=i['UserProfileName'],
                                     domain_id=domain_id)
            self.create_user_profile(user_profile_name=i['UserProfileName'],
                                     execution_role=i['UserSettings']['ExecutionRole'],
                                     domain_id=domain_id,
                                     **kwargs)

    def list_apps(self,
                  domain_id: Optional[str] = None,
                  **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        response = self.cli.list_apps(DomainIdEquals=domain_id,
                                      SortBy='CreationTime',
                                      SortOrder='Descending',
                                      MaxResults=100,
                                      **kwargs)
        return response['Apps']

    def describe_app(self,
                     user_profile_name: str,
                     app_name: str,
                     app_type: str,
                     domain_id: Optional[str] = None,
                     **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            return self.cli.describe_app(DomainId=domain_id,
                                         UserProfileName=user_profile_name,
                                         AppName=app_name,
                                         AppType=app_type,
                                         **kwargs)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{user_profile_name}: {app_name} does not exist')
            return None

    def create_app(self,
                   user_profile_name: str,
                   app_type: str,
                   app_name: str,
                   domain_id: Optional[str] = None,
                   **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            self.cli.create_app(DomainId=domain_id,
                                UserProfileName=user_profile_name,
                                AppType=app_type,
                                AppName=app_name,
                                **kwargs)
            while self.describe_app(user_profile_name=user_profile_name, app_name=app_name,
                                    app_type=app_type)['Status'] == 'Pending':
                time.sleep(10)
            self.logger.info(f'{user_profile_name}: {app_type}-{app_name} created')
        except self.cli.exceptions.ResourceInUse:
            self.logger.info(f'{user_profile_name}: {app_type}-{app_name} already exists')
            return None

    def delete_app(self,
                   user_profile_name: str,
                   app_name: str,
                   app_type: str,
                   domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            self.cli.delete_app(DomainId=domain_id,
                                UserProfileName=user_profile_name,
                                AppName=app_name,
                                AppType=app_type)
            while self.describe_app(user_profile_name=user_profile_name, app_name=app_name,
                                    app_type=app_type)['Status'] != 'Deleted':
                time.sleep(10)
            self.logger.info(f'{user_profile_name}: {app_type}-{app_name} deleted')
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{user_profile_name}: {app_type}-{app_name} does not exist')
            return None

    def list_images(self,
                    max_results: Optional[int] = 100):
        return self.cli.list_images(MaxResults=max_results)['Images']

    def describe_image(self,
                       image_name: str):
        try:
            return self.cli.describe_image(ImageName=image_name)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info('ResourceNotFound')
            return None

    def create_image(self,
                     image_name: str,
                     role_arn: str,
                     **kwargs):
        try:
            self.cli.create_image(ImageName=image_name,
                                  RoleArn=role_arn,
                                  **kwargs)
            while self.describe_image(image_name=image_name)['ImageStatus'] == 'CREATING':
                time.sleep(5)
            self.logger.info(f'{image_name} created')
        except self.cli.exceptions.ResourceInUse:
            self.logger.info(f'{image_name} already exists')
            return None

    def delete_image(self,
                     image_name: str):
        try:
            self.cli.delete_image(ImageName=image_name)
            while image_name in [x['ImageName'] for x in self.list_images()]:
                time.sleep(5)
            self.logger.info(f'{image_name} deleted')
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{image_name} does not exist')
            return None

    def list_image_versions(self,
                            image_name: str,
                            max_results: Optional[int] = 100):
        try:
            return self.cli.list_image_versions(ImageName=image_name, MaxResults=max_results)['ImageVersions']
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{image_name} does not exist')
            return None

    def describe_image_version(self,
                               image_name: str,
                               **kwargs):
        try:
            return self.cli.describe_image_version(ImageName=image_name, **kwargs)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'image version of {image_name} does not exist')
            return None

    def create_image_version(self,
                             base_image_uri: str,
                             image_name: str,
                             **kwargs):
        try:
            self.cli.create_image_version(BaseImage=base_image_uri,
                                          ImageName=image_name,
                                          **kwargs)
            while self.describe_image_version(image_name=image_name)['ImageVersionStatus'] == 'CREATING':
                time.sleep(5)
            self.logger.info(f"{image_name}: version {self.describe_image_version(image_name=image_name)['Version']}"
                             f" created")
        except self.cli.exceptions.ResourceInUse:
            self.logger.info(f"{image_name}: version {self.describe_image_version(image_name=image_name)['Version']}"
                             f"already exists")
            return None

    def delete_image_version(self,
                             image_name: str,
                             version: int):
        try:
            self.cli.delete_image_version(ImageName=image_name, Version=version)
            while version in [x['Version'] for x in self.list_image_versions(image_name=image_name)]:
                time.sleep(5)
            self.logger.info(f'{image_name}: version {version} deleted')
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'{image_name}: version {version} does not exist')
            return None
