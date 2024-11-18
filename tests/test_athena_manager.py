import os

import pytest

from baram.s3_manager import S3Manager
from baram.glue_manager import GlueManager
from baram.athena_manager import AthenaManager


@pytest.fixture()
def am():
    return AthenaManager(query_result_bucket_name='sli-dst-athena-public',
                         output_bucket_name='sli-dst-dlprod-public',
                         workgroup='adw_etl')


@pytest.fixture()
def gm():
    return GlueManager(s3_bucket_name='sli-dst-dlprod-public')


@pytest.fixture()
def sm():
    return S3Manager(bucket_name='sli-dst-dlprod-public')


@pytest.fixture()
def sample():
    return {'db_name': 'sample',
            's3_dirpath': 'incoming/sample/third/',
            'table_name': 'sample_table',
            's3_filename': 'sample_table.txt',
            's3_filepath': 'incoming/sample/third/sample_table/once',
            'file_content': 'col1,col2\ntest,2',
            'partitioned_table_name': 'sample_partitioned_table',
            'partitioned_s3_filename': 'sample_partitioned_table.txt',
            'partitioned_s3_filepath': 'incoming/sample/third/sample_partitioned_table/once',
            'partitioned_file_content': 'date,col1,col2\n2024-01-01,test,1\n2024-01-02,test,2',
            'column_def': {'col1': 'string', 'col2': 'int'},
            'column_comments': {'col1': 'column1'},
            'partition_cols': {'date': 'date'},
            'named_query_name': 'sample_named_query',
            'named_query_string': 'select * from sample.sample_table'}


def set_test_basis(am: AthenaManager,
                   sm: S3Manager,
                   sample: dict,
                   is_partitioned: bool = False):
    crit = 'partitioned_' if is_partitioned else ''
    sm.write_and_upload_file(sample[f'{crit}file_content'],
                             sample[f'{crit}s3_filename'],
                             f"{sample[f'{crit}s3_filepath']}/{sample[f'{crit}s3_filename']}")
    os.remove(sample[f'{crit}s3_filename'])
    location = sm.get_s3_full_path(sm.bucket_name, sample[f'{crit}s3_filepath'])

    s3_output = sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                    f"{am.ATHENA_WORKGROUP}/once/tables/{sample[f'{crit}table_name']}")

    return location, s3_output


def set_named_query_basis(am: AthenaManager,
                          sample: dict):
    names_before = am.get_named_query_names()
    if sample['named_query_name'] in names_before:
        am.delete_named_query_by_name(query_name=sample['named_query_name'])


def test_create_external_table(am, sm, gm, sample):
    # Given
    am.delete_table_if_exists(db_name=sample['db_name'], table_name=sample['table_name'])
    location, _ = set_test_basis(am, sm, sample)

    # When
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name'])

    result = gm.get_table(db_name=sample['db_name'], table_name=sample['table_name'])
    rslt_cols = result['StorageDescriptor']['Columns']

    assert result['DatabaseName'] == sample['db_name']
    assert result['StorageDescriptor']['Location'] == location
    assert {x['Name']: x['Type'] for x in rslt_cols} == sample['column_def']
    sm.delete_dir(sample['s3_filepath'])


def test_create_external_table_with_partitioning(am, sm, gm, sample):
    # Given
    location, _ = set_test_basis(am, sm, sample, is_partitioned=True)

    # When
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['partitioned_table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/"
                                                           f"{sample['partitioned_table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1',
                             partition_cols=sample['partition_cols'])

    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['partitioned_table_name'])

    result = gm.get_table(db_name=sample['db_name'], table_name=sample['partitioned_table_name'])
    rslt_cols = result['StorageDescriptor']['Columns']
    rslt_partitions = result['PartitionKeys']

    assert result['DatabaseName'] == sample['db_name']
    assert result['StorageDescriptor']['Location'] == location
    assert {x['Name']: x['Type'] for x in rslt_cols} == sample['column_def']
    assert {x['Name']: x['Type'] for x in rslt_partitions} == sample['partition_cols']
    sm.delete_dir(sample['s3_filepath'])


def test_delete_table(am, sm, sample):
    # Given
    location, _ = set_test_basis(am, sm, sample)
    if not am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']):
        am.create_external_table(db_name=sample['db_name'],
                                 table_name=sample['table_name'],
                                 column_def=sample['column_def'],
                                 location=location,
                                 s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                               f"{am.ATHENA_WORKGROUP}/once/tables"
                                                               f"/{sample['table_name']}"),
                                 column_comments=sample['column_comments'],
                                 table_comment='table1')

    # When
    am.delete_table(db_name=sample['db_name'], table_name=sample['table_name'])

    # Then
    assert sm.list_objects(location) is None
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']) is False


def test_fetch_query(am, sm, sample):
    # Given
    location, s3_output = set_test_basis(am, sm, sample)
    if not am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name']):
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
    sm.delete_dir(sample['s3_filepath'])


def test_count_rows_from_table(am, sm, sample):
    # Given
    location, _ = set_test_basis(am, sm, sample)
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    # When
    result = am.count_rows_from_table(db_name=sample['db_name'], table_name=sample['table_name'])
    df = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                              db_name=sample['db_name'])

    # Then
    assert result == df.shape[0]
    sm.delete_dir(sample['s3_filepath'])


def test_optimize_and_vacumm_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_optimize_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_vacumm_iceberg_table(am, sample):
    # TODO: TBD, create temp table/data and check it
    assert False


def test_check_table_exists(am, sm, sample):
    # Given
    location, _ = set_test_basis(am, sm, sample)

    # When
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    # Then
    assert am.check_table_exists(db_name=sample['db_name'], table_name=sample['table_name'])
    sm.delete_dir(sample['s3_filepath'])


def test_read_query_txt(am, sm, sample):
    # Given
    # TODO: TBD, create temp table/data and check it

    # When

    # Then
    assert False


def test_from_athena_to_df(am, sm, sample):
    # Given
    location, s3_output = set_test_basis(am, sm, sample)
    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=s3_output,
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    # When
    df = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                              db_name=sample['db_name'])

    # Then
    import pandas as pd
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(sample['column_def'].keys())
    assert df.shape[0] == sample['file_content'].count('\n')
    assert df.shape[1] == len(sample['column_def'].keys())
    sm.delete_dir(sample['s3_filepath'])


def test_get_iceberg_metadata_df(am, sm, sample):
    # Given
    am.delete_table_if_exists(db_name=sample['db_name'], table_name=sample['table_name'])
    location, _ = set_test_basis(am, sm, sample)

    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    to_db_name, to_table_name = sample['db_name'], f"{sample['table_name']}_iceberg"
    am.delete_table_if_exists(db_name=to_db_name, table_name=to_table_name)

    am.create_iceberg_table_from_table(to_db_name=to_db_name,
                                       to_table_name=to_table_name,
                                       as_sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                       location=f"s3://{am.OUTPUT_BUCKET}/table/{to_db_name}.db/{to_table_name}")

    # When
    df_file = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='files')
    df_mnfs = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='manifests')
    df_hist = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='history')
    df_ptns = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='partitions')
    df_snap = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='snapshots')
    df_refs = am.get_iceberg_metadata_df(db_name=to_db_name, table_name=to_table_name, property='refs')

    # Then
    assert 'file_path' in df_file.columns
    assert 'path' in df_mnfs.columns and 'added_snapshot_id' in df_mnfs.columns
    assert 'made_current_at' in df_hist.columns
    assert 'total_size' in df_ptns.columns and 'data' in df_ptns.columns
    assert 'manifest_list' in df_snap.columns
    assert 'type' in df_refs.columns


def test_get_table_manifest_paths(am, sm, sample):
    # Given
    am.delete_table_if_exists(db_name=sample['db_name'], table_name=sample['table_name'])
    location, _ = set_test_basis(am, sm, sample)

    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    to_db_name, to_table_name = sample['db_name'], f"{sample['table_name']}_iceberg"
    am.delete_table_if_exists(db_name=to_db_name, table_name=to_table_name)

    am.create_iceberg_table_from_table(to_db_name=to_db_name,
                                       to_table_name=to_table_name,
                                       as_sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                       location=f"s3://{am.OUTPUT_BUCKET}/table/{to_db_name}.db/{to_table_name}")

    # When
    manifest_paths = am.get_table_manifest_paths(db_name=to_db_name, table_name=to_table_name)

    # Then
    print(f'manifest_paths: {manifest_paths}')
    assert len([path for path in manifest_paths if path.split('.')[-1] == 'avro']) == len(manifest_paths)


def test_create_iceberg_table_from_table(am, sm, gm, sample):
    # Given
    am.delete_table_if_exists(db_name=sample['db_name'], table_name=sample['table_name'])
    location, _ = set_test_basis(am, sm, sample)

    am.create_external_table(db_name=sample['db_name'],
                             table_name=sample['table_name'],
                             column_def=sample['column_def'],
                             location=location,
                             s3_output=sm.get_s3_full_path(am.QUERY_RESULT_BUCKET,
                                                           f"{am.ATHENA_WORKGROUP}/once/tables/{sample['table_name']}"),
                             column_comments=sample['column_comments'],
                             table_comment='table1')

    to_db_name, to_table_name = sample['db_name'], f"{sample['table_name']}_iceberg"
    am.delete_table_if_exists(db_name=to_db_name, table_name=to_table_name)

    # When
    am.create_iceberg_table_from_table(to_db_name=sample['db_name'],
                                       to_table_name=f"{sample['table_name']}_iceberg",
                                       as_sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                       location=f"s3://{am.OUTPUT_BUCKET}/table/{to_db_name}.db/{to_table_name}")

    df_before = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{sample['table_name']}",
                                     db_name=sample['db_name'])

    df_after = am.from_athena_to_df(sql=f"select * from {sample['db_name']}.{to_table_name}",
                                    db_name=sample['db_name'])

    # TODO: add assertions
    # Then
    print(f'df_before={df_before}')
    print(f'df_after={df_after}')


def test_create_named_query(am, sample):
    # Given
    set_named_query_basis(am, sample)

    # When
    am.create_named_query(query_name=sample['named_query_name'],
                          sql=sample['named_query_string'],
                          db_name=sample['db_name'])
    # Then
    names_after = am.get_named_query_names()
    assert sample['named_query_name'] in names_after

    info = am.get_named_query_info_by_name(query_name=sample['named_query_name'])
    assert sample['named_query_string'] == info['QueryString']


def test_get_named_query_infos(am, sample):
    # Given
    set_named_query_basis(am, sample)
    am.create_named_query(query_name=sample['named_query_name'],
                          sql=sample['named_query_string'],
                          db_name=sample['db_name'])

    # When
    infos = am.get_named_query_infos()

    # Then
    assert len(infos) == 1
    assert len(infos.keys) == 5
    assert infos[0]['Name'] == sample['named_query_name']
    assert infos[0]['Database'] == sample['db_name']
    assert infos[0]['WorkGroup'] == am.ATHENA_WORKGROUP
    assert infos[0]['QueryString'] == sample['named_query_string']


def test_get_named_query_names(am, sample):
    # Given
    set_named_query_basis(am, sample)
    am.create_named_query(query_name=sample['named_query_name'],
                          sql=sample['named_query_string'],
                          db_name=sample['db_name'])

    # When
    names = am.get_named_query_names()

    # Then
    assert len(names) == 1
    assert names[0] == sample['named_query_name']


def test_get_named_query_string_with_name(am, sample):
    # Given
    set_named_query_basis(am, sample)
    am.create_named_query(query_name=sample['named_query_name'],
                          sql=sample['named_query_string'],
                          db_name=sample['db_name'])

    # When
    sql = am.get_named_query_string_by_name(query_name=sample['named_query_name'])

    # Then
    assert sql == sample['named_query_string']


# TODO
def test_fetch_named_query(am, sample):
    # Given
    # When
    # Then
    pass
