import os

import boto3
from botocore.client import Config

from baram.log_manager import LogManager
from baram.kms_manager import KMSManager


class S3Manager(object):
    def __init__(self, bucket_name, kms_id):
        self.cli = boto3.client('s3', config=Config(signature_version='s3v4'))
        self.km = KMSManager()
        self.logger = LogManager.get_logger()
        self.bucket_name = bucket_name
        self.kms_id = kms_id

    def list_buckets(self):
        '''
        :return: response
        '''
        response = self.cli.list_buckets()
        return response['Buckets'] if 'Buckets' in response else None

    def put_object(self, s3_key_id: str, body):
        '''

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :param body: byte or str data
        :return: response
        '''

        response = self.cli.put_object(Bucket=self.bucket_name,
                                       Key=s3_key_id,
                                       Body=body,
                                       ServerSideEncryption='aws:kms',
                                       SSEKMSKeyId=self.kms_id)
        return response

    def get_object(self, s3_key_id: str):
        '''

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''
        try:
            response = self.cli.get_object(Bucket=self.bucket_name,
                                           Key=s3_key_id)
            return response['Body'].read()
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def get_object_by_lines(self, s3_key_id: str):
        '''
        get s3 object line by line.

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''

        try:
            import codecs
            line_stream = codecs.getreader("utf-8")
            response = self.cli.get_object(Bucket=self.bucket_name,
                                           Key=s3_key_id)
            return line_stream(response['Body'])
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def delete_object(self, s3_key_id: str):
        '''

        :param s3_key_id: s3 key id. ex) nylon-detector/a.csv
        :return: response
        '''
        response = self.cli.delete_object(Bucket=self.bucket_name,
                                          Key=s3_key_id)
        return response

    def upload_dir(self, local_dir_path: str, s3_dir_path: str):
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
                    dest_path = path.replace(local_dir_path, '')
                    s3file_path = os.path.normpath(s3_dir_path + '/' + dest_path + '/' + file)
                    local_file_path = os.path.join(path, file)
                    self.cli.upload_file(local_file_path,
                                         self.bucket_name,
                                         s3file_path,
                                         ExtraArgs={'ServerSideEncryption': 'aws:kms',
                                                    'SSEKMSKeyId': self.kms_id})
                    self.logger.info(
                        f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def upload_file(self, local_file_path: str, s3file_path: str):
        '''
        Upload file.
        :param local_file_path: local file path. ex) /Users/lks21c/repo/sli-aflow/a.csv
        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data/a.csv
        :return: response
        '''

        try:
            self.cli.upload_file(local_file_path,
                                 self.bucket_name,
                                 s3file_path,
                                 ExtraArgs={'ServerSideEncryption': 'aws:kms',
                                            'SSEKMSKeyId': self.kms_id})
            self.logger.info(f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def download_dir(self, s3_dir_path: str, local_dir_path: str = os.getcwd()):
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

    def delete_dir(self, s3_dir_path: str):
        '''
        Delete s3 directory.

        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data
        :return:
        '''
        for k in self.list_objects(s3_dir_path):
            self.delete_object(k['Key'])
        self.delete_object(s3_dir_path)

    def download_file(self, s3_file_path: str, local_file_path: str):
        '''
        Download file from s3.
        \
        :param s3_dir_path: s3 path. ex) nylon-detector/crawl_data/a.csv
        :param local_file_path: local file path. ex) /Users/lks21c/repo/sli-aflow/a.csv
        :return: response
        '''
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        bucket.download_file(s3_file_path, local_file_path)
        self.logger.info(f'download : {s3_file_path} to Target: {local_file_path} Success.')

    def list_objects(self, prefix: str = '', delimiter: str = ''):
        '''
        List S3 objects.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :return: response
        '''
        response = self.cli.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter)
        return response['Contents'] if 'Contents' in response else None

    def list_dir(self, prefix: str = '', delimiter: str = '/'):
        '''
        List directory.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :return: response
        '''
        response = self.cli.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter)
        list = []
        if 'Contents' in response:
            list += [item['Key'] for item in response['Contents'] if 'Key' in item]
        if 'CommonPrefixes' in response:
            list += [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        return list

    def get_s3_arn(self, bucket_name):
        '''

        :param bucket_name: bucket name
        :return: s3 bucket arn
        '''
        return next(
            (f'arn:aws:s3:::{i["Name"]}' for i in self.list_buckets() if bucket_name in i['Name']),
            None)
