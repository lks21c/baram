import os.path
import shutil
import tempfile

import pytest

from baram.s3_manager import S3Manager


@pytest.fixture()
def sm():
    return S3Manager('sli-dst-dlbeta-public')


@pytest.fixture()
def sample():
    return {'s3_bucket_name': 'sli-dst-dlbeta-public', 's3_key': 'readme.md'}


def test_list_buckets(sm):
    # When
    for b in sm.list_buckets():
        assert b['Name']
        print(f'BucketName: b{b["Name"]}, CreationDate: {b["CreationDate"]}')


def test_put_get_delete_object(sm, sample):
    # Given
    s3_body = 'hello world'

    # When/Then
    sm.put_object(sample['s3_key'], s3_body)
    assert sm.get_object(sample['s3_key']).decode() == s3_body

    sm.delete_object(sample['s3_key'])
    assert sm.get_object(sample['s3_key']) is None


def test_get_object_by_lines(sm, sample):
    # Given
    s3_body = 'hello world'
    sm.put_object(sample['s3_key'], s3_body)

    # When
    for l in sm.get_object_by_lines(sample['s3_key']):
        assert l
        print(l)

    # Then
    sm.delete_object(sample['s3_key'])
    assert sm.get_object_by_lines(sample['s3_key']) is None


def test_upload_download_delete_dir(sm, sample):
    # Given
    temp_dir = tempfile.mkdtemp()
    temp_file = tempfile.mkstemp(dir=temp_dir)
    tmp_filename = temp_file[1].split('/')[-1]
    s3_tmp_dir = 'tmp_dir'

    # When/Then
    sm.upload_dir(
        temp_dir,
        s3_tmp_dir)

    file_exist = False
    for f in sm.list_objects(f'{s3_tmp_dir}/'):
        if f['Key'].split('/')[-1] == tmp_filename:
            file_exist = True
    assert file_exist

    sm.download_dir(s3_tmp_dir)
    assert os.path.exists(os.path.join(s3_tmp_dir, tmp_filename))
    shutil.rmtree(s3_tmp_dir)

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
    # Given
    # TODO: Create Dirs.

    # When
    for dir in sm.list_dir('dirs/', '/'):
        assert dir
        print(dir)

    # Then
    # TODO: Check dirs.


def test_get_bucket_encryption(sm, sample):
    # When
    bi = sm.get_bucket_encryption()

    # Then
    assert bi
    print(bi['SSEAlgorithm'])
    print(bi['KMSMasterKeyID'])


def test_get_s3_web_url(sm):
    # When
    url = sm.get_s3_web_url('bucket_name', 'a/b')

    # Then
    assert url == 'https://s3.console.aws.amazon.com/s3/buckets/bucket_name?region=ap-northeast-2&prefix=a/b'


def test_convert_s3_path_to_web_url(sm):
    # When
    url = sm.convert_s3_path_to_web_url('s3://bucket_name/a/b')

    # Then
    assert url == 'https://s3.console.aws.amazon.com/s3/buckets/bucket_name?region=ap-northeast-2&prefix=a/b'


def test_get_s3_full_path(sm):
    # When
    path = sm.get_s3_full_path('bucket_name', 'a/b')

    # Then
    assert path == 's3://bucket_name/a/b'


def test_check_s3_object_exists(sm, sample):
    # Given
    s3_body = 'hello world'
    sm.put_object(sample['s3_key'], s3_body)

    # When
    check = sm.check_s3_object_exists(sample['s3_bucket_name'], sample['s3_key'])

    # Then
    assert check
    sm.delete_object(sample['s3_key'])


def test_rename_file(sm):
    # TODO: change tc using temp file.
    assert False


def test_count_csv_row_count(sm):
    # TODO: change tc using temp file.
    assert False


def test_copy(sm):
    # TODO: change tc using temp file.
    # Given
    assert False

    # When
    sm.copy(
        from_key='test/case1.csv',
        to_key='test/case3.csv')

    # Then
    # TODO: TBD


def test_copy_object(sm):
    # TODO: change tc using temp file.

    # Given
    assert False

    # When
    sm.copy_object(
        from_key='incoming/prod_mydata_master/first/iu001/daily/2023/10/iu001_20231005.csv/part-00000-00aa4d98-f751-4c07-ae08-3e7b7402ed10-c000.csv',
        to_key='incoming/prod_mydata_master/first/iu001/daily/2023/10/iu001_20231005_001.csv')

    # Then
    # TODO: TBD
