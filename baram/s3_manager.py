import os
from codecs import StreamReader
from logging import Logger
from typing import Optional, Union, Any

import boto3
from botocore.client import Config, BaseClient

from baram.log_manager import LogManager
from baram.kms_manager import KMSManager


class S3Manager(object):
    def __init__(self, bucket_name: str) -> None:
        self.cli: BaseClient = boto3.client('s3', config=Config(signature_version='s3v4'))
        self.km: KMSManager = KMSManager()
        self.logger: Logger = LogManager.get_logger('S3Manager')
        self.bucket_name: str = bucket_name
        try:
            bi: Optional[dict] = self.get_bucket_encryption()
            self.kms_algorithm: Optional[str] = bi['SSEAlgorithm']
            self.kms_id: Optional[str] = bi['KMSMasterKeyID']
        except:
            self.kms_algorithm, self.kms_id = None, None

    def list_buckets(self) -> Optional[dict]:
        '''
        :return: response
        '''
        response: dict = self.cli.list_buckets()
        return response['Buckets'] if 'Buckets' in response else None

    def put_object(self, s3_key_id: str, body: Union[bytes, str]) -> dict:
        '''

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :param body: byte or str data
        :return: response
        '''
        kwargs: dict = {"Bucket": self.bucket_name,
                        "Key": s3_key_id,
                        "Body": body}
        if self.kms_id:
            kwargs['ServerSideEncryption'] = self.kms_algorithm
            kwargs['SSEKMSKeyId'] = self.kms_id
        response: dict = self.cli.put_object(**kwargs)
        return response

    def get_object(self, s3_key_id: str) -> Optional[bytes]:
        '''
        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''
        try:
            response: dict = self.cli.get_object(Bucket=self.bucket_name,
                                                 Key=s3_key_id)
            return response['Body'].read()
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def get_object_by_lines(self, s3_key_id: str) -> StreamReader:
        '''
        get s3 object line by line.

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''

        try:
            import codecs
            line_stream: StreamReader = codecs.getreader("utf-8")
            response: dict = self.cli.get_object(Bucket=self.bucket_name,
                                                 Key=s3_key_id)
            return line_stream(response['Body'])
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def delete_object(self, s3_key_id: str) -> dict:
        '''

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''
        return self.cli.delete_object(Bucket=self.bucket_name,
                                      Key=s3_key_id)

    def delete_objects(self, s3_keys: list, quiet: bool = True) -> dict:
        objects = [{'Key': k} for k in s3_keys]
        return self.cli.delete_objects(Bucket=self.bucket_name,
                                       Delete={
                                           'Objects': objects,
                                           'Quiet': quiet
                                       })

    def upload_dir(self, local_dir_path: str, s3_dir_path: str) -> None:
        '''
        Upload directory.
        :param local_dir_path: local dir path. ex) /Users/lks21c/repo/sli-aflow
        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data
        :return: response
        '''
        self.logger.info('Uploading results to s3 initiated...')
        self.logger.info(f'local_path:{local_dir_path}, s3_path:{s3_dir_path}')
        try:
            for path, subdirs, files in os.walk(local_dir_path):
                for file in files:
                    dest_path: str = path.replace(local_dir_path, '')
                    s3file_path: Union[str, Any] = os.path.normpath(s3_dir_path + '/' + dest_path + '/' + file)
                    local_file_path: bytes = os.path.join(path, file)

                    extra_args: dict = {'ServerSideEncryption': self.kms_algorithm,
                                        'SSEKMSKeyId': self.kms_id} if self.kms_id else None
                    self.cli.upload_file(local_file_path,
                                         self.bucket_name,
                                         s3file_path,
                                         ExtraArgs=extra_args)
                    self.logger.info(
                        f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def upload_file(self, local_file_path: str, s3_file_path: str) -> None:
        '''
        Upload file.
        :param s3_file_path:
        :param local_file_path: local file path. ex) /Users/lks21c/repo/sli-aflow/a.csv
        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data/a.csv
        :return: response
        '''

        try:
            extra_args: dict = {'ServerSideEncryption': self.kms_algorithm,
                          'SSEKMSKeyId': self.kms_id} if self.kms_id else None
            self.cli.upload_file(local_file_path,
                                 self.bucket_name,
                                 s3_file_path,
                                 ExtraArgs=extra_args)
            self.logger.info(f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3_file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def download_dir(self, s3_dir_path: str, local_dir_path: str = os.getcwd()) -> None:
        '''
        Download directory from s3.

        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data
        :param local_dir_path: local dir path. ex) /Users/lks21c/repo/sli-aflow
        :return: response
        '''
        self.logger.info('Downloading results to s3 initiated...')
        self.logger.info(f's3_path:{s3_dir_path}, local_path:{local_dir_path}')
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        for obj in bucket.objects.filter(Prefix=s3_dir_path):
            local_obj_path = os.path.join(local_dir_path, obj.key)
            if not os.path.exists(os.path.dirname(local_obj_path)):
                os.makedirs(os.path.dirname(local_obj_path))
            bucket.download_file(obj.key, local_obj_path)
            self.logger.info(f'download : {obj.key} to Target: {local_obj_path} Success.')

    def delete_dir(self, s3_dir_path: str) -> None:
        '''
        Delete s3 directory.

        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data
        :return:
        '''
        files = self.list_objects(s3_dir_path)
        if not files:
            return
        s3_keys = []
        for k in self.list_objects(s3_dir_path):
            s3_keys.append(k['Key'])
            if len(s3_keys) % 1000 == 0:
                self.logger.info(f'delete 1000 keys.')
                self.delete_objects(s3_keys)
                s3_keys = []
        if len(s3_keys) > 0:
            self.logger.info(f'delete {len(s3_keys)} keys.')
            self.delete_objects(s3_keys)
        self.logger.info(f'delete {s3_dir_path}')

    def download_file(self, s3_file_path: str, local_file_path: str):
        '''
        Download file from s3.
        \
        :param s3_file_path: s3 path. ex) nylon-detector/crawl_data/a.csv
        :param local_file_path: local file path. ex) /Users/lks21c/repo/sli-aflow/a.csv
        :return: response
        '''
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        bucket.download_file(s3_file_path, local_file_path)
        self.logger.info(f'download : {s3_file_path} to Target: {local_file_path} Success.')

    def list_objects(self, prefix: str = '', delimiter: str = '') -> Optional[list]:
        '''
        List S3 objects.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :return: response
        '''
        response: dict = self.cli.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter)
        return response['Contents'] if 'Contents' in response else None

    def list_dir(self, prefix: str = '', delimiter: str = '/') -> list:
        '''
        List directory.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :return: response
        '''
        response: dict = self.cli.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter)
        list: list = []
        if 'Contents' in response:
            list += [item['Key'] for item in response['Contents'] if 'Key' in item]
        if 'CommonPrefixes' in response:
            list += [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        return list

    def get_s3_arn(self, bucket_name: str) -> Optional[str]:
        '''

        :param bucket_name: bucket name
        :return: s3 bucket arn
        '''
        return next(
            (f'arn:aws:s3:::{i["Name"]}' for i in self.list_buckets() if bucket_name in i['Name']),
            None)

    def get_bucket_encryption(self) -> Optional[dict]:
        '''
        :return: KMS ID
        '''
        conf: dict = self.cli.get_bucket_encryption(Bucket=self.bucket_name)['ServerSideEncryptionConfiguration']
        return conf['Rules'][0]['ApplyServerSideEncryptionByDefault'] if conf else None
