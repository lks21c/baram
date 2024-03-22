import time
from typing import Optional

import boto3

from baram.log_manager import LogManager


class SagemakerManager(object):
    def __init__(self, domain_id: str = None):
        self.cli = boto3.client('sagemaker')
        self.domain_id = domain_id
        self.logger = LogManager.get_logger('SagemakerManager')

    def list_user_profiles(self,
                           domain_id: Optional[str] = None,
                           **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        response = self.cli.list_user_profiles(DomainIdEquals=domain_id,
                                               **kwargs)
        return response['UserProfiles']

    def describe_user_profile(self,
                              user_profile_name: str,
                              domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        response = self.cli.describe_user_profile(DomainId=domain_id,
                                                  UserProfileName=user_profile_name)
        return response

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

    def delete_app(self,
                   user_profile_name: str,
                   app_name: str,
                   app_type: str,
                   domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            response = self.cli.delete_app(DomainId=domain_id,
                                           UserProfileName=user_profile_name,
                                           AppName=app_name,
                                           AppType=app_type)
            return response
        except:
            return None

    def describe_app(self,
                     user_profile_name: str,
                     app_name: str,
                     app_type: str,
                     domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        response = self.cli.describe_app(DomainId=domain_id,
                                         UserProfileName=user_profile_name,
                                         AppName=app_name,
                                         AppType=app_type)
        return response

    def create_user_profile(self,
                            user_profile_name: str,
                            execution_role: str,
                            domain_id: Optional[str] = None,
                            is_sso_domain: Optional[bool] = False,
                            sso_user_value: Optional[str] = None,
                            **kwargs):
        domain_id = domain_id if domain_id else self.domain_id
        self.logger.info(f'start creating {user_profile_name}')
        if is_sso_domain:
            response = self.cli.create_user_profile(DomainId=domain_id,
                                                    UserProfileName=user_profile_name,
                                                    UserSettings={
                                                        'ExecutionRole': execution_role},
                                                    SingleSignOnUserIdentifier='UserName',
                                                    SingleSignOnUserValue=sso_user_value,
                                                    **kwargs)
        else:
            response = self.cli.create_user_profile(DomainId=domain_id,
                                                    UserProfileName=user_profile_name,
                                                    UserSettings={
                                                        'ExecutionRole': execution_role},
                                                    **kwargs)
        return response

    def delete_user_profile(self,
                            user_profile_name: str,
                            domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        try:
            self.describe_user_profile(user_profile_name=user_profile_name,
                                       domain_id=domain_id)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info(f'user {user_profile_name} does not exist.')
            return

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
        return self.cli.delete_user_profile(DomainId=domain_id, UserProfileName=user_profile_name)

    def recreate_all_user_profiles(self,
                                   is_sso_domain: Optional[bool] = False,
                                   domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        user_profiles = [self.describe_user_profile(user_profile_name=x['UserProfileName'], domain_id=domain_id)
                         for x in self.list_user_profiles(domain_id=domain_id)]
        self.logger.info(f"user profiles to recreate: {[x['UserProfileName'] for x in user_profiles]}")

        for i in user_profiles:
            self.logger.info(f"start deleting {i['UserProfileName']}")
            self.delete_user_profile(user_profile_name=i['UserProfileName'],
                                     domain_id=domain_id)
            while i['UserProfileName'] in self.list_user_profiles(domain_id=domain_id):
                time.sleep(5)
            else:
                self.logger.info(f"{i['UserProfileName']} deleted")
                time.sleep(5)
                if is_sso_domain:
                    self.create_user_profile(user_profile_name=i['UserProfileName'],
                                             execution_role=i['UserSettings']['ExecutionRole'],
                                             domain_id=domain_id,
                                             is_sso_domain=is_sso_domain,
                                             sso_user_value=i['SingleSignOnUserValue'])
                else:
                    self.create_user_profile(user_profile_name=i['UserProfileName'],
                                             execution_role=i['UserSettings']['ExecutionRole'],
                                             domain_id=domain_id)
            self.logger.info(f"{i['UserProfileName']} created")

    def list_domains(self):
        return self.cli.list_domains()['Domains']

    def delete_domain(self,
                      domain_id: Optional[str] = None):
        domain_id = domain_id if domain_id else self.domain_id
        response = self.cli.delete_domain(DomainId=domain_id, RetentionPolicy={'HomeEfsFileSystem': 'Delete'})
        return response

    def create_domain(self,
                      domain_name: str,
                      execution_role_name: str,
                      sg_group: str,
                      s3_kms_id: str,
                      efs_kms_id: str,
                      s3_output_path: str,
                      vpc_id: str,
                      subnet_id1: str,
                      subnet_id2: str,
                      instance_type: str = 'ml.t3.micro'):
        pass
        # TODO: TBD.
        response = self.cli.create_domain(
            DomainName=domain_name,
            AuthMode='IAM',
            DefaultUserSettings={
                'ExecutionRole': execution_role_name,
                'SecurityGroups': [
                    sg_group,
                ],
                'SharingSettings': {
                    'NotebookOutputOption': 'Allowed',
                    'S3OutputPath': s3_output_path,
                    'S3KmsKeyId': s3_kms_id
                },
                'JupyterServerAppSettings': {
                    'DefaultResourceSpec': {
                        'SageMakerImageArn': 'string',
                        'SageMakerImageVersionArn': 'string',
                        'InstanceType': instance_type
                    }
                },
                'KernelGatewayAppSettings': {
                    'DefaultResourceSpec': {
                        'SageMakerImageArn': 'string',
                        'SageMakerImageVersionArn': 'string',
                        'InstanceType': instance_type
                    },
                    'CustomImages': [
                        {
                            'ImageName': 'string',
                            'ImageVersionNumber': 1,
                            'AppImageConfigName': 'string'
                        },
                    ],
                    'LifecycleConfigArns': [
                        'string',
                    ]
                }
            },
            SubnetIds=[
                'string',
            ],
            VpcId=vpc_id,
            Tags=[
                {
                    'Key': 'string',
                    'Value': 'string'
                },
            ],
            AppNetworkAccessType='VpcOnly',
            KmsKeyId=efs_kms_id,
            AppSecurityGroupManagement='Service' | 'Customer',
            DomainSettings={
                'SecurityGroupIds': [
                    'string',
                ]
            }
        )

    def describe_image(self, image_name):
        try:
            return self.cli.describe_image(ImageName=image_name)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info('ResourceNotFound')
            return None

    def describe_image_version(self, image_name):
        try:
            return self.cli.describe_image_version(ImageName=image_name)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info('ResourceNotFound')
            return None

    def create_image_version(self, image_uri: str, name: str):
        return self.cli.create_image_version(
            BaseImage=image_uri,
            ImageName=name
        )

    def delete_image(self, image_name):
        try:
            return self.cli.delete_image(ImageName=image_name)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info('ResourceNotFound')
            return None

    def delete_image_version(self, image_name, version):
        try:
            return self.cli.delete_image_version(ImageName=image_name, Version=version)
        except self.cli.exceptions.ResourceNotFound:
            self.logger.info('ResourceNotFound')
            return None
