import pytest

from baram.s3_manager import S3Manager
from baram.athena_manager import AthenaManager


@pytest.fixture()
def am():
    return AthenaManager(bucket_name='sli-dst-dlprod-public',
                         workgroup='adw_etl')


@pytest.fixture()
def sm():
    return S3Manager(bucket_name='sli-dst-dlprod-public')


@pytest.fixture()
def sample():
    return {'db_name': 'sample', 'table_name': 'sample_table'}


def test_create_glue_external_table(am, sm, sample):
    # Given
    column_def = {'col1': 'string', 'col2': 'int'}
    column_comments = {'col1': 'column1'}
    location = sm.get_s3_full_path(sm.bucket_name, 'query_results')

    # When
    am.create_glue_external_table(db_name=sample['db_name'],
                                  table_name=sample['table_name'],
                                  column_def=column_def,
                                  location=location,
                                  column_comments=column_comments,
                                  table_comment='table1')
    # Then
    pass


def test_delete_glue_table(am, sm, sample):
    # Given
    am.fetch_query(sql=f"create external table if not exists {sample['db_name']}.{sample['table_name']}("
                       f"   col1 string comment 'column1', "
                       f"   col2 int comment 'column2') "
                       f"comment 'sample table' "
                       f"row format delimited fields terminated by ',' "
                       f"stored as textfile "
                       f"location '{sm.get_s3_full_path(sm.bucket_name, 'query_results')}' "
                       f"tblproperties ('classification'='csv');",
                   db_name=sample['db_name'],
                   s3_output=sm.get_s3_full_path(sm.bucket_name, 'query_results'))

    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name'])

    # When
    am.delete_glue_table(db_name=sample['db_name'],
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
