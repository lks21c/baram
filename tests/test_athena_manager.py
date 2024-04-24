import os
from pprint import pprint

import pytest

from baram.s3_manager import S3Manager
from baram.glue_manager import GlueManager
from baram.athena_manager import AthenaManager


@pytest.fixture()
def am():
    return AthenaManager(query_result_bucket_name='sli-dst-athena-public',
                         workgroup='adw_etl')


@pytest.fixture()
def gm():
    return GlueManager('sli-dst-dlprod-public')


@pytest.fixture()
def sm():
    return S3Manager(bucket_name='sli-dst-dlprod-public')


@pytest.fixture()
def sample():
    return {'db_name': 'sample',
            'table_name': 'sample_table',
            's3_filename': 'sample_table.txt',
            's3_filepath': 'incoming/sample/third/sample_table/once',
            'column_def': {'col1': 'string', 'col2': 'int'},
            'column_comments': {'col1': 'column1'}}


def test_create_external_table(am, sm, gm, sample):
    # Given
    files = [x['Key'].split('/')[-1] for x in sm.list_objects(prefix=sample['s3_filepath'])]
    if sample['s3_filename'] not in files:
        with open(sample['s3_filename'], "w") as file:
            file.write('col1,col2\ntest,2')

        sm.upload_file(sample['s3_filename'], f"{sample['s3_filepath']}/{sample['s3_filename']}")
        os.remove(sample['s3_filename'])

    location = sm.get_s3_full_path(sm.bucket_name, sample['s3_filepath'])
    s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                    f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}")

    # When
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=s3_output,
                             column_comments=sample['column_comments'],
                             table_comment='table1')
    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name'])

    result = gm.get_table(db_name=sample['db_name'], table_name=sample['table_name'])
    result_cols = result['StorageDescriptor']['Columns']
    result_col_names, result_col_types = [x['Name'] for x in result_cols], [x['Type'] for x in result_cols]

    assert result['DatabaseName'] == sample['db_name']
    assert result['StorageDescriptor']['Location'] == location
    assert len(result_cols) == len(sample['column_def'])
    assert result_col_names == list(sample['column_def'].keys())
    assert result_col_types == list(sample['column_def'].values())


def test_delete_table(am, sm, sample):
    # Given
    files = [x['Key'].split('/')[-1] for x in sm.list_objects(prefix=sample['s3_filepath'])]
    if sample['s3_filename'] not in files:
        with open(sample['s3_filename'], "w") as file:
            file.write('col1,col2\ntest,2')

        sm.upload_file(sample['s3_filename'], f"{sample['s3_filepath']}/{sample['s3_filename']}")
        os.remove(sample['s3_filename'])

    if not am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']):
        location = sm.get_s3_full_path(sm.bucket_name, 'query_results')
        s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET, f"{am.ATHENA_WORKGROUP}")
        am.create_external_table(db_name=sample['db_name'],
                                 table_name=sample['table_name'],
                                 column_def=sample['column_def'],
                                 location=location,
                                 s3_output=s3_output,
                                 column_comments=sample['column_comments'],
                                 table_comment='table1')

    # When
    am.delete_table(db_name=sample['db_name'],
                    table_name=sample['table_name'])

    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']) is False


def test_fetch_query(am, sm, sample):
    # Given
    files = [x['Key'].split('/')[-1] for x in sm.list_objects(prefix=sample['s3_filepath'])]
    if sample['s3_filename'] not in files:
        with open(sample['s3_filename'], "w") as file:
            file.write('col1,col2\ntest,2')

        sm.upload_file(sample['s3_filename'], f"{sample['s3_filepath']}/{sample['s3_filename']}")
        os.remove(sample['s3_filename'])

    s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET, f"{am.ATHENA_WORKGROUP}")
    if not am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']):
        location = sm.get_s3_full_path(sm.bucket_name, 'query_results')
        am.create_external_table(db_name=sample['db_name'],
                                 table_name=sample['table_name'],
                                 column_def=sample['column_def'],
                                 location=location,
                                 s3_output=s3_output,
                                 column_comments=sample['column_comments'],
                                 table_comment='table1')

    before = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                  db_name=sample['db_name'])

    # When
    sql = f"insert into {sample['db_name']}.{sample['table_name']} values ('insert test', 99)"
    response = am.fetch_query(sql=sql, db_name=sample['db_name'], s3_output=s3_output)
    after = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                 db_name=sample['db_name'])

    # Then
    assert sql == response['Query']
    assert before.shape != after.shape
    assert before.shape[-1] == after.shape[-1]
    assert abs(before.shape[0] - after.shape[0]) == 1


def test_count_rows_from_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_optimize_and_vacumm_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_optimize_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_vacumm_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_check_table_exists(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_read_query_txt(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_from_athena_to_df(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False
