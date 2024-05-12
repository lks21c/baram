import csv
import os.path
import shutil
import tempfile

import pytest

from baram.s3_manager import S3Manager
from tests.json_manager import JsonManager


@pytest.fixture()
def sample():
    conf = JsonManager.load_json(os.path.join(os.path.dirname(__file__), 'conf', 'test_info.json'))
    return {'s3_bucket_name': conf['s3_bucket_name'], 's3_key': 'readme.md'}


@pytest.fixture()
def sm(sample):
    return S3Manager(sample['s3_bucket_name'])


def test_list_buckets(sm):
    # When
    response = sm.list_buckets()

    # Then
    for b in response:
        assert b['Name']
        print(f'BucketName: b{b["Name"]}, CreationDate: {b["CreationDate"]}')


def test_put_get_delete_object(sm, sample):
    # Given
    s3_body = 'hello world'

    # When/Then
    sm.put_object(sample['s3_key'], s3_body)
    assert sm.get_object_body(sample['s3_key']).decode() == s3_body

    sm.delete_object(sample['s3_key'])
    assert sm.get_object_body(sample['s3_key']) is None


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
    sm.upload_dir(temp_dir, s3_tmp_dir)

    file_exist = False
    for resp in sm.list_objects(f'{s3_tmp_dir}/'):
        for f in resp['Contents']:
            if f['Key'].split('/')[-1] == tmp_filename:
                file_exist = True
    assert file_exist

    sm.download_dir(s3_tmp_dir)
    assert os.path.exists(os.path.join(s3_tmp_dir, tmp_filename))
    shutil.rmtree(s3_tmp_dir)

    sm.delete_dir(s3_tmp_dir)
    assert len(sm.list_dir(prefix=s3_tmp_dir)) == 0


def test_upload_download_delete_file(sm, sample):
    # Given
    temp_file = tempfile.mkstemp()
    s3_tmp_file = 'tmp_file'

    # When
    sm.upload_file(temp_file[1], s3_tmp_file)

    # Then
    for resp in sm.list_objects(s3_tmp_file):
        for obj in resp['Contents']:
            assert obj['Key'] == s3_tmp_file

    sm.download_file(s3_tmp_file, s3_tmp_file)
    assert os.path.exists(s3_tmp_file)
    os.remove(s3_tmp_file)

    sm.delete_object(s3_tmp_file)
    assert sm.get_object_body(s3_tmp_file) is None


def test_write_and_upload_file(sm):
    # Given
    content = 'hello world'
    tmp_dir = tempfile.mkdtemp()
    local_file_path = os.path.join(tmp_dir, 'tmp_file.txt')
    s3_file_path = 'tmp_file'

    # When
    sm.write_and_upload_file(content=content,
                             local_file_path=local_file_path,
                             s3_file_path=s3_file_path,
                             do_remove=True)

    # Then
    for resp in sm.list_objects(s3_file_path):
        for obj in resp['Contents']:
            assert obj['Key'] == s3_file_path

    sm.delete_object(s3_file_path)


def test_delete_objects(sm, sample):
    # Given
    temp_file, temp_file2 = tempfile.mkstemp(), tempfile.mkstemp()
    s3_tmp_file, s3_tmp_file2 = 'tmp_file', 'tmp_file2'

    sm.upload_file(temp_file[1], s3_tmp_file)

    sm.upload_file(temp_file2[1], s3_tmp_file2)

    # When
    sm.delete_objects([s3_tmp_file, s3_tmp_file2])

    # Then
    assert sm.get_object_body(s3_tmp_file) is None
    assert sm.get_object_body(s3_tmp_file2) is None


def test_list_dir(sm):
    # Given
    s3_dir = 'temp_dir/'
    s3_key_id = f'{s3_dir}temp_file.txt'
    s3_body = 'hello world'

    sm.put_object(s3_key_id, s3_body)

    # When
    response = sm.list_dir(prefix=s3_dir)

    # Then
    assert len(response) == 1
    assert response[0] == s3_key_id
    print(response)

    sm.delete_dir(s3_key_id)


def test_get_s3_arn(sm, sample):
    # Given
    bucket_name = sample['s3_bucket_name']

    # When
    response = sm.get_s3_arn(bucket_name)

    # Then
    assert response == f'arn:aws:s3:::{bucket_name}'


def test_get_bucket_encryption(sm, sample):
    # When
    bi = sm.get_bucket_encryption()

    # Then
    print(bi)
    assert bi
    print(bi['SSEAlgorithm'])
    if 'KMSMasterKeyID' in bi:
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


def test_rename_file(sm, sample):
    # Given
    s3_body = 'hello world'
    sm.put_object(sample['s3_key'], s3_body)
    to_file_path = 'readme_2.md'

    # When
    sm.rename_file(from_file_path=sample['s3_key'],
                   to_file_path=to_file_path)

    # Then
    check = sm.check_s3_object_exists(sample['s3_bucket_name'], to_file_path)
    assert check

    sm.delete_object(to_file_path)


def test_read_csv_from_s3(sm):
    # Given
    local_tmp_file = tempfile.mkstemp()[1]
    header = ['col1', 'col2']
    data = [['a', 1], ['b', 2], ['c', 3], ['d', 4]]

    with open(local_tmp_file, 'w') as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in data:
            w.writerow(row)

    s3_tmp_file = 'tmp_file.csv'
    sm.upload_file(local_tmp_file, s3_tmp_file)

    # When
    df = sm.read_csv_from_s3(s3_tmp_file)

    # Then
    print(df)

    sm.delete_object(s3_tmp_file)


def test_write_csv_to_s3(sm):
    # Given
    local_tmp_file = tempfile.mkstemp()[1]
    header = ['col1', 'col2']
    data = [['a', 1], ['b', 2], ['c', 3], ['d', 4]]

    with open(local_tmp_file, 'w') as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in data:
            w.writerow(row)

    s3_tmp_file = 'tmp_file.csv'
    sm.upload_file(local_tmp_file, s3_tmp_file)
    df = sm.read_csv_from_s3(s3_tmp_file)

    # When
    s3_tmp_file2 = 'tmp_file2.csv'
    sm.write_csv_to_s3(df, s3_tmp_file2)

    # Then
    df2 = sm.read_csv_from_s3(s3_tmp_file2)
    assert df.compare(df2).empty

    sm.delete_object(s3_tmp_file)
    sm.delete_object(s3_tmp_file2)


def test_count_csv_row_count(sm):
    # Given
    import csv

    local_tmp_file = 'tmp.csv'
    header = ['col1', 'col2']
    data = [['a', 1], ['b', 2], ['c', 3], ['d', 4]]

    with open(local_tmp_file, 'w') as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in data:
            w.writerow(row)

    s3_tmp_file = 'tmp_file'
    sm.upload_file(local_tmp_file, s3_tmp_file)

    # When
    s3_tmp_row_cnt = sm.count_csv_row_count(csv_path=s3_tmp_file)

    # Then
    assert s3_tmp_row_cnt == len(data)

    os.remove(local_tmp_file)
    sm.delete_object(s3_tmp_file)


def test_copy(sm, sample):
    # Given
    s3_body = 'hello world'
    from_key = sample['s3_key']
    to_key = 'readme_2.md'

    sm.put_object(from_key, s3_body)

    # When
    sm.copy(from_key=from_key,
            to_key=to_key)

    # Then
    from_check = sm.check_s3_object_exists(sample['s3_bucket_name'], from_key)
    assert from_check

    to_check = sm.check_s3_object_exists(sample['s3_bucket_name'], to_key)
    assert to_check

    sm.delete_objects([from_key, to_key])


def test_copy_object(sm, sample):
    # Given
    s3_body = 'hello world'
    from_key = sample['s3_key']
    to_key = 'readme_2.md'

    sm.put_object(from_key, s3_body)

    # When
    sm.copy_object(from_key=from_key,
                   to_key=to_key)

    # Then
    assert sm.check_s3_object_exists(sample['s3_bucket_name'], from_key)

    assert sm.check_s3_object_exists(sample['s3_bucket_name'], to_key)

    sm.delete_objects([from_key, to_key])


def test_list_object(sm, sample):
    # Given
    for i in range(1, 11):
        temp_file = tempfile.mkstemp()
        s3_tmp_file = f'test_list_object/tmp_file{i}'

        sm.upload_file(temp_file[1], s3_tmp_file)

    # When
    resp_iter = sm.list_objects('test_list_object/')

    # Then
    for resp in resp_iter:
        print(resp['Contents'])
        assert len(resp['Contents']) == 10

    sm.delete_dir('test_list_object')


def test_delete_dir(sm, sample):
    # Given
    temp_file = tempfile.mkstemp()
    s3_tmp_file = f'test_list_object/tmp_file'
    sm.upload_file(temp_file[1], s3_tmp_file)

    # When
    sm.delete_dir('')

    # Then
    for resp in sm.list_objects(''):
        assert resp['KeyCount'] == 0
