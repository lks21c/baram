import os
import tempfile
from typing import Any, Optional

import boto3
import botocore
import avro.schema
import pandas as pd
import awswrangler as wr
from botocore.client import Config
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter

from baram.kms_manager import KMSManager
from baram.log_manager import LogManager


class S3Manager(object):
    def __init__(self, bucket_name):
        if 'my_bucket' == bucket_name:
            raise ValueError('Please set your bucket name.')
        self.cli = boto3.client('s3', config=Config(signature_version='s3v4'))
        self.km = KMSManager()
        self.logger = LogManager.get_logger('S3Manager')
        self.bucket_name = bucket_name
        try:
            bi = self.get_bucket_encryption()
            self.kms_algorithm, self.kms_id = bi['SSEAlgorithm'], bi['KMSMasterKeyID']
        except:
            self.kms_algorithm, self.kms_id = None, None

    def list_buckets(self):
        """
        List all S3 buckets.

        :return: response
        """
        response = self.cli.list_buckets()
        return response['Buckets'] if 'Buckets' in response else None

    def put_object(self, s3_key_id: str, body):
        """
        Put an object to S3.

        :param s3_key_id: S3 key id. ex) dir/a.csv
        :param body: byte or str data
        :return: response
        """
        kwargs = {"Bucket": self.bucket_name,
                  "Key": s3_key_id,
                  "Body": body}
        if self.kms_id:
            kwargs['ServerSideEncryption'] = self.kms_algorithm
            kwargs['SSEKMSKeyId'] = self.kms_id
        response = self.cli.put_object(**kwargs)
        return response

    def get_object_body(self, s3_key_id: str):
        """
        Get specified object in S3.

        :param s3_key_id: S3 key id. ex) dir/a.csv
        :return: response
        """
        try:
            response = self.cli.get_object(Bucket=self.bucket_name,
                                           Key=s3_key_id)
            return response['Body'].read()
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def get_object_by_lines(self, s3_key_id: str, encoding: str = 'utf-8'):
        """
        Get S3 object line by line.

        :param s3_key_id: S3 key id. ex) dir/a.csv
        :param encoding: encoding, default utf-8
        :return: response
        """

        try:
            import codecs
            line_stream = codecs.getreader(encoding)
            response = self.cli.get_object(Bucket=self.bucket_name,
                                           Key=s3_key_id)
            return line_stream(response['Body'])
        except self.cli.exceptions.NoSuchKey:
            self.logger.info(f'{s3_key_id} does not exist.')
            return None

    def delete_object(self, s3_key_id: str):
        """
        Delete specified object in S3.

        :param s3_key_id: S3 key id. ex) dir/a.csv
        :return: response
        """
        return self.cli.delete_object(Bucket=self.bucket_name,
                                      Key=s3_key_id)

    def delete_objects(self, s3_keys: list, quiet: bool = True):
        """
        Delete list of objects in S3.

        :param s3_keys: list of S3 key ids.
        :param quiet: True if using quiet mode, False if using verbose mode.
        :return:
        """
        objects = [{'Key': k} for k in s3_keys]
        response = self.cli.delete_objects(Bucket=self.bucket_name,
                                           Delete={
                                               'Objects': objects,
                                               'Quiet': quiet
                                           })
        if 'Errors' in response and len(response['Errors']) > 0:
            raise Exception(response['Errors'])
        return response

    def upload_dir(self, local_dir_path: str, s3_dir_path: str):
        """
        Upload local directory to S3.

        :param local_dir_path: local directory path. ex) /Users/JohnDoe/dataset
        :param s3_dir_path: S3 path. ex) dir/crawl_data
        :return: response
        """
        self.logger.info('Uploading results to s3 initiated...')
        self.logger.info(f'local_path:{local_dir_path}, s3_path:{s3_dir_path}')
        try:
            for path, subdirs, files in os.walk(local_dir_path):
                for file in files:
                    dest_path = path.replace(local_dir_path, '')
                    s3_file_path = os.path.normpath(s3_dir_path + '/' + dest_path + '/' + file)
                    local_file_path = os.path.join(path, file)

                    extra_args = {'ServerSideEncryption': self.kms_algorithm,
                                  'SSEKMSKeyId': self.kms_id} if self.kms_id else None
                    self.cli.upload_file(local_file_path, self.bucket_name, s3_file_path, ExtraArgs=extra_args)
                    self.logger.info(
                        f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3_file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def write_and_upload_file(self, content: str, s3_file_path: str, do_remove: bool = False):
        """
        Write and upload file to S3.

        :param content: the content of file. ex) 'col1,col2\name,height'
        :param s3_file_path: S3 path. ex) dir/crawl_data/a.csv
        :param do_remove: remove written file
        :return: response
        """

        temp_file = tempfile.mkstemp()
        local_file_path = temp_file[1]
        with open(local_file_path, 'w') as f:
            f.write(content)
        assert os.path.exists(local_file_path)
        self.upload_file(local_file_path, s3_file_path)

        if do_remove:
            os.remove(local_file_path)

    def upload_file(self, local_file_path: str, s3_file_path: str):
        """
        Upload file to S3.

        :param local_file_path: local file path. ex) /Users/JohnDoe/dataset/a.csv
        :param s3_file_path: S3 path. ex) dir/crawl_data/a.csv
        :return: response
        """

        try:
            extra_args = {'ServerSideEncryption': self.kms_algorithm,
                          'SSEKMSKeyId': self.kms_id} if self.kms_id else None
            self.cli.upload_file(local_file_path, self.bucket_name, s3_file_path, ExtraArgs=extra_args)
            self.logger.info(f'upload : {local_file_path} to Target: s3://{self.bucket_name}/{s3_file_path} Success.')
        except Exception as e:
            self.logger.info(e)
            raise e

    def download_dir(self, s3_dir_path: str, local_dir_path: str = os.getcwd()):
        """
        Download S3 directory to local directory path.

        :param s3_dir_path: S3 path. ex) dir/crawl_data
        :param local_dir_path: local directory path. ex) /Users/JohnDoe/dataset
        :return: response
        """
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
        """
        Delete S3 directory.

        :param s3_dir_path: S3 path. ex) dir/crawl_data
        :return:
        """
        s3_keys = []
        for resp in self.list_objects(s3_dir_path):
            for k in resp['Contents']:
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
        """
        Download file from S3.

        :param s3_file_path: S3 path. ex) dir/crawl_data/a.csv
        :param local_file_path: local file path. ex) /Users/JohnDoe/dataset/a.csv
        :return: response
        """
        bucket = boto3.resource('s3').Bucket(self.bucket_name)
        bucket.download_file(s3_file_path, local_file_path)
        self.logger.info(f'download : {s3_file_path} to Target: {local_file_path} Success.')

    def list_objects(self, prefix: str = '', delimiter: str = '', **kwargs):
        """
        List S3 objects.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :param kwargs: Additional parameters
        :return: response
        """

        paginator = self.cli.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=self.bucket_name,
                                  Prefix=prefix,
                                  Delimiter=delimiter,
                                  **kwargs)

    def list_dir(self, prefix: str = '', delimiter: str = '/'):
        """
        List S3 directory.

        :param prefix: Limits the response to keys that begin with the specified prefix.
        :param delimiter: A delimiter is a character you use to group keys.
        :return: response
        """
        response = self.cli.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter)
        lst = []
        if 'Contents' in response:
            lst += [item['Key'] for item in response['Contents'] if 'Key' in item]
        if 'CommonPrefixes' in response:
            lst += [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        return lst

    def get_s3_arn(self, bucket_name):
        """
        Get S3 bucket's ARN

        :param bucket_name: bucket name
        :return: S3 bucket arn
        """
        return next(
            (f'arn:aws:s3:::{i["Name"]}' for i in self.list_buckets() if bucket_name in i['Name']),
            None)

    def get_bucket_encryption(self):
        """
        Get S3 bucket's encryption key

        :return: KMS ID
        """
        conf = self.cli.get_bucket_encryption(Bucket=self.bucket_name)['ServerSideEncryptionConfiguration']
        return conf['Rules'][0]['ApplyServerSideEncryptionByDefault'] if conf else None

    def copy(self, from_key: str, to_key: str, to_bucket: Optional[str] = None):
        """
        Creates a copy of an object that is already stored in Amazon S3.

        :param from_key: origin S3 key
        :param to_key: destination S3 key
        :param to_bucket: destination S3 bucket
        :return:
        """

        copy_source = {
            'Bucket': self.bucket_name,
            'Key': from_key
        }
        to_bucket = to_bucket if to_bucket else self.bucket_name
        self.cli.copy(copy_source, to_bucket, to_key)

    def copy_object(self, from_key: str, to_key: str):
        """
        Creates a copy of an object that is already stored in Amazon S3.

        :param from_key: origin S3 key
        :param to_key: destination S3 key
        :return:
        """
        self.cli.copy_object(
            Bucket=self.bucket_name,
            CopySource=f'{self.bucket_name}/{from_key}',
            Key=to_key
        )

    def get_s3_web_url(self, s3_bucket_name, path: str, region: str = 'ap-northeast-2'):
        """
        Get S3 bucket's web url

        :param s3_bucket_name: s3 bucket name
        :param path: s3 path
        :param region: s3 region
        :return:
        """
        return f'https://s3.console.aws.amazon.com/s3/buckets/{s3_bucket_name}?region={region}&prefix={path}'

    def convert_s3_path_to_web_url(self, s3_path: str):
        """
        Convert S3 url to web url

        :param s3_path:
        :return:
        """
        token = s3_path.replace('s3://', '').split('/')
        return self.get_s3_web_url(token[0], '/'.join(token[1:]))

    def get_s3_full_path(self, s3_bucket_name: str, path: str):
        """
        Get S3 bucket's full path.

        :param s3_bucket_name: bucket name
        :param path: path
        :return:
        """
        return f's3://{s3_bucket_name}/{path}'

    def check_s3_object_exists(self, s3_bucket_name: str, path: str):
        """
        Check if S3 object exists.

        :param s3_bucket_name: s3 bucket name
        :param path: path
        :return:
        """
        try:
            self.cli.head_object(Bucket=s3_bucket_name,
                                 Key=path)
            return True
        except botocore.exceptions.ClientError as e:
            pass
        return False

    def rename_file(self, from_file_path: str, to_file_path: str):
        """
        Rename S3 object

        :param from_file_path: origin file path
        :param to_file_path: destination file path
        :return:
        """
        self.copy_object(from_key=from_file_path, to_key=to_file_path)
        self.delete_dir(from_file_path)

    def read_csv_from_s3(self,
                         s3_file_path: str,
                         **pandas_kwargs: Any):
        """
        Read csv file in S3.

        :param s3_file_path: csv file path in S3
        :param pandas_kwargs:
            KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass pandas_kwargs explicitly,
            just add valid Pandas arguments in the function call and awswrangler will accept it.
            e.g. wr.s3.read_csv(‘s3://bucket/prefix/’, sep=’|’, na_values=[‘null’, ‘none’], skip_blank_lines=True)
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        :return: pandas Dataframe
        """
        df = wr.s3.read_csv(path=f's3://{self.bucket_name}/{s3_file_path}',
                            index_col=False, keep_default_na=False, **pandas_kwargs)
        return df

    def write_csv_to_s3(self,
                        df: pd.DataFrame,
                        s3_file_path: str,
                        **pandas_kwargs: Any):
        """
        Write csv file to S3.

        :param df: pandas DataFrame
        :param s3_file_path: target S3 file path to write csv file
        :param pandas_kwargs:
            KEYWORD arguments forwarded to pandas.DataFrame.to_csv(). You can NOT pass pandas_kwargs explicit,
            just add valid Pandas arguments in the function call and awswrangler will accept it.
            e.g. wr.s3.to_csv(df, path, sep=’|’, na_rep=’NULL’, decimal=’,’)
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
        :return:
        """

        extra_args = {'ServerSideEncryption': self.kms_algorithm,
                      'SSEKMSKeyId': self.kms_id} if self.kms_id else None
        wr.s3.to_csv(df=df, path=f's3://{self.bucket_name}/{s3_file_path}', index=False,
                     s3_additional_kwargs=extra_args, **pandas_kwargs)

    def merge_csvs(self,
                   source_path: str,
                   target_path: str,
                   chunksize: int = 1000000,
                   **pandas_kwargs):
        """
        Merge source csv file into target csv file.
        :param source_path: S3 path of source csv file
        :param target_path: S3 path of target csv file
        :param chunksize: number of rows to append at a time
        :param pandas_kwargs:
            KEYWORD arguments forwarded to pandas.DataFrame.to_csv().
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
        :return:
        """

        source_df = self.read_csv_from_s3(s3_file_path=source_path)
        source_df.to_csv(path_or_buf=f's3://{self.bucket_name}/{target_path}', header=False, index=False,
                         mode='a', chunksize=chunksize, **pandas_kwargs)

    def count_csv_row_count(self, csv_path: str, distinct_col_name: Optional[str] = None):
        """
        Count the number of lines in a csv file.

        :param csv_path: csv file path
        :param distinct_col_name: specific column to count rows
        :return: row count
        """
        df = wr.s3.read_csv(path=f's3://{self.bucket_name}/{csv_path}', index_col=False,
                            keep_default_na=False)

        if distinct_col_name:
            return len(pd.unique(df[distinct_col_name]))
        else:
            return df.shape[0]

    def get_avro_as_list(self, avro_path: str):
        """

        :param avro_path:
        :return:
        """
        filename = avro_path.split('/')[-1]
        assert filename.split('.')[-1] != 'avro'

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file_path = f'{tmpdir}/{filename}'
            self.download_file(s3_file_path=avro_path,
                               local_file_path=local_file_path)
            reader = DataFileReader(open(local_file_path, 'rb'), DatumReader())

        return list(reader)
