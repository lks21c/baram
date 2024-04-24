import os

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
    return {'db_name': 'sample', 'table_name': 'sample_table'}


def test_create_external_table(am, sm, gm, sample):
    # Given
    local_filepath = f"{sample['table_name']}.txt"
    s3_filepath = f"incoming/{sample['db_name']}/third/{sample['table_name']}/once"

    with open(local_filepath, "w") as file:
        file.write('col1,col2\ntest,2')

    sm.upload_file(local_filepath, f"{s3_filepath}/{local_filepath}")
    os.remove(local_filepath)

    column_def, column_comments = {'col1': 'string', 'col2': 'int'}, {'col1': 'column1'}
    location = sm.get_s3_full_path(sm.bucket_name, s3_filepath)
    s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                    f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}")

    # When
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=column_def,
                             location=location,
                             s3_output=s3_output,
                             column_comments=column_comments,
                             table_comment='table1')
    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name'])

    result = gm.get_table(db_name=sample['db_name'], table_name=sample['table_name'])['Table']
    result_col_names = [x['Name'] for x in result['StorageDescriptor']['Columns']]
    result_col_types = [x['Type'] for x in result['StorageDescriptor']['Columns']]

    assert result['DatabaseName'] == sample['db_name']
    assert result['StorageDescriptor']['Location'] == location
    assert result_col_names == list(column_def.keys())
    assert result_col_types == list(column_def.values())


def test_delete_table(am, sm, sample):
    # Given
    column_def = {'col1': 'string', 'col2': 'int'}
    column_comments = {'col1': 'column1'}
    location = sm.get_s3_full_path(sm.bucket_name, 'query_results')
    s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET, f"{am.ATHENA_WORKGROUP}")

    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=column_def,
                             location=location,
                             s3_output=s3_output,
                             column_comments=column_comments,
                             table_comment='table1')

    # When
    am.delete_table(db_name=sample['db_name'],
                    table_name=sample['table_name'])

    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']) is False


def test_fetch_query(am, sample):
    # Given
    # TODO: create a table and insert sample data

    # When
    response = am.fetch_query(f"SELECT * FROM {sample['table_name']}", sample['db_name'])

    # Then
    # TODO: assert response


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
