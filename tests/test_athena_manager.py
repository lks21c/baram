import pytest

from baram.athena_manager import AthenaManager


@pytest.fixture()
def am():
    return AthenaManager()


@pytest.fixture()
def sample():
    return {'db_name': 'sample_db', 'table_name': 'sample_table'}


def test_delete_glue_table(am, sample):
    # Given
    # TODO: create a table

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
