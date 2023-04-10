import os.path
import shutil
import tempfile

import pytest

from baram.s3_manager import S3Manager


@pytest.fixture()
def sm():
    return S3Manager('sli-dst-dldev')


@pytest.fixture()
def sample():
    return {'s3_key': 'readme.md'}


def test_list_buckets(sm):
    for b in sm.list_buckets():
        assert b['Name']
        print(f'BucketName: b{b["Name"]}, CreationDate: {b["CreationDate"]}')


def test_put_get_delete_object(sm, sample):
    s3_body = 'hello world'

    sm.put_object(sample['s3_key'], s3_body)
    assert sm.get_object(sample['s3_key']).decode() == s3_body

    sm.delete_object(sample['s3_key'])
    assert sm.get_object(sample['s3_key']) is None


# def test_list_objects(sm, sample):
#     for obj in sm.list_objects('nylon-detector/crawl_data/백내장+부수입/url_original', ''):
#         assert obj
#         print(f'Key: {obj["Key"]}, LastModified: {obj["LastModified"]}')


def test_get_object_by_lines(sm, sample):
    s3_body = 'hello world'

    sm.put_object(sample['s3_key'], s3_body)
    for l in sm.get_object_by_lines(sample['s3_key']):
        assert l
        print(l)

    sm.delete_object(sample['s3_key'])
    assert sm.get_object_by_lines(sample['s3_key']) is None


def test_upload_download_delete_dir(sm, sample):
    temp_dir = tempfile.mkdtemp()
    temp_file = tempfile.mkstemp(dir=temp_dir)
    tmp_filename = temp_file[1].split('/')[-1]
    s3_tmp_dir = 'tmp_dir'

    sm.upload_dir(
        temp_dir,
        s3_tmp_dir)

    # s3 file check
    file_exist = False
    for f in sm.list_objects(f'{s3_tmp_dir}/'):
        if f['Key'].split('/')[-1] == tmp_filename:
            file_exist = True
    assert file_exist

    # download
    sm.download_dir(s3_tmp_dir)
    assert os.path.exists(os.path.join(s3_tmp_dir, tmp_filename))
    shutil.rmtree(s3_tmp_dir)

    # delete
    sm.delete_dir(s3_tmp_dir)


def test_upload_download_delete_file(sm, sample):
    # Given
    temp_file = tempfile.mkstemp()
    s3_tmp_file = 'tmp_file'

    # When
    sm.upload_file(
        temp_file[1],
        s3_tmp_file)

    # Then
    for obj in sm.list_objects(s3_tmp_file):
        assert obj['Key'] == s3_tmp_file

    sm.download_file(s3_tmp_file, s3_tmp_file)
    assert os.path.exists(s3_tmp_file)
    os.remove(s3_tmp_file)

    sm.delete_object(s3_tmp_file)
    assert sm.get_object(s3_tmp_file) is None


def test_delete_objects(sm, sample):
    # Given
    temp_file, temp_file2 = tempfile.mkstemp(), tempfile.mkstemp()
    s3_tmp_file, s3_tmp_file2 = 'tmp_file', 'tmp_file2'

    sm.upload_file(
        temp_file[1],
        s3_tmp_file)

    sm.upload_file(
        temp_file2[1],
        s3_tmp_file2)

    # When
    sm.delete_objects([s3_tmp_file, s3_tmp_file2])

    # Then
    assert sm.get_object(s3_tmp_file) is None
    assert sm.get_object(s3_tmp_file2) is None


def test_list_dir(sm, sample):
    for dir in sm.list_dir('nylon-detector/', '/'):
        assert dir
        print(dir)


def test_get_bucket_encryption(sm, sample):
    bi = sm.get_bucket_encryption()
    assert bi
    print(bi['SSEAlgorithm'])
    # print(bi['KMSMasterKeyID'])


def test_upload_download_delete_dir(sm, sample):
    temp_dir = tempfile.mkdtemp()
    temp_file = tempfile.mkstemp(dir=temp_dir)
    tmp_filename = temp_file[1].split('/')[-1]
    s3_tmp_dir = 'tmp_dir'

    sm.upload_dir(
        temp_dir,
        s3_tmp_dir)

    # s3 file check
    file_exist = False
    for f in sm.list_objects(f'{s3_tmp_dir}/'):
        if f['Key'].split('/')[-1] == tmp_filename:
            file_exist = True
    assert file_exist

    # download
    sm.download_dir(s3_tmp_dir)
    assert os.path.exists(os.path.join(s3_tmp_dir, tmp_filename))
    shutil.rmtree(s3_tmp_dir)

    # delete
    sm.delete_dir(s3_tmp_dir)
