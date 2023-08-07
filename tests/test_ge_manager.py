from pprint import pprint

import pytest
from baram.ge_manager import GEManager
from great_expectations.core import ExpectationConfiguration


@pytest.fixture()
def gm():
    return GEManager(gx_s3_bucket_name='sli-dst-gxbeta-public',
                     data_s3_bucket_name='sli-dst-dlbeta-public')


@pytest.fixture()
def sample():
    return {'suite_name_empty': 'titanic_empty_expectation',
            'suite_name_profile': 'titanic_profile',
            'suite_name_one_expectation': 'titanic_one_expectation',
            'suite_name_one_expectation_athena': 'titanic_one_expectation_athena',
            'suite_name_two_expectation': 'titanic_two_expectation',
            's3_asset_name': 'titanic.csv',
            'athena_database_name': 'sample',
            'athena_table_name': 'titanic'
            }


@pytest.fixture()
def suite_empty(gm, sample):
    suite = gm.make_suite(sample['suite_name_empty'])
    yield suite
    gm.delete_suite(sample['suite_name_empty'])


@pytest.fixture()
def suite_profile(gm, sample):
    suite = gm.make_suite(sample['suite_name_profile'])
    yield suite
    gm.delete_suite(sample['suite_name_profile'])


@pytest.fixture()
def suite_one_expectation(gm, sample):
    gm.make_suite(sample['suite_name_one_expectation'])
    ecs = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Sex",
                "value_set": ['male', 'female']
            }
        )
    ]
    suite = gm.add_expectations(sample['suite_name_one_expectation'], ecs)
    yield suite
    gm.delete_suite(sample['suite_name_one_expectation'])


@pytest.fixture()
def suite_one_expectation_athena(gm, sample):
    gm.make_suite(sample['suite_name_one_expectation_athena'])
    ecs = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "sex",
                "value_set": ['male', 'female']
            }
        )
    ]
    suite = gm.add_expectations(sample['suite_name_one_expectation_athena'], ecs)
    yield suite
    gm.delete_suite(sample['suite_name_one_expectation_athena'])


@pytest.fixture()
def suite_two_expectation(gm, sample):
    gm.make_suite(sample['suite_name_two_expectation'])
    ecs = [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Sex",
                "value_set": ['male', 'female']
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "PClass",
                "value_set": ["1st", "2nd", "3rd"]
            }
        )
    ]
    suite = gm.add_expectations(sample['suite_name_two_expectation'], ecs)
    yield suite
    gm.delete_suite(sample['suite_name_two_expectation'])


def test_make_suite(gm, sample, suite_empty):
    # Then
    assert suite_empty is not None
    print(suite_empty)


def test_list_expectation_suites(gm, sample, suite_empty, suite_one_expectation):
    # When
    suites = gm.list_expectation_suites()

    # Then
    assert len(suites) > 0
    print(suites)


def test_add_expectation(gm, sample, suite_empty):
    # Given
    ecs = [
        ExpectationConfiguration(
            expectation_type="expect_column_mean_to_be_between",
            kwargs={
                "column": "Age",
                "min_value": "20",
                "max_value": "40"
            }
        )
    ]

    # When
    gm.add_expectations(sample['suite_name_empty'], ecs)

    # Then
    suite = gm.get_suite_by_name(sample['suite_name_empty'])
    assert suite is not None
    print(suite)


def test_with_pandas_csv(gm):
    # Given
    df = gm.context.sources.pandas_default.read_csv(
        "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    )

    # Then
    df.expect_column_values_to_not_be_null("pickup_datetime")
    df.save_expectation_suite()


def test_s3_asset(gm, sample):
    # When
    data_asset = gm.add_s3_pandas_csv_dataset(sample['s3_asset_name'])

    # Then
    assert data_asset is not None


def test_batch_list(gm, sample):
    # When
    batch_list = gm._get_batch_list(sample['s3_asset_name'])

    # Then
    assert len(batch_list) > 0


# TODO: 편리하게 suite 만들기
def test_make_convinient_suite(gm, sample, suite_empty):
    # Given
    batch_list = gm._get_batch_list(sample['s3_asset_name'])
    validator = gm._get_validator(batch_list=batch_list, suite=suite_empty)

    # When
    validator.expect_column_values_to_not_be_null("Age")

    # Then
    validator.save_expectation_suite()
    validator.validate()
    print(suite_empty)


def test_validate_s3(gm, sample, suite_one_expectation):
    # Given
    batch_list = gm._get_batch_list(sample['s3_asset_name'])

    # When
    result = gm.validate(batch_list=batch_list, suite=suite_one_expectation)

    # Then
    assert result['success']
    for r in result['results']:
        pprint(r['result'])


def test_validate_athena(gm, sample, suite_one_expectation_athena):
    # Given
    batch_list = gm._get_batch_list(athena_database_name=sample['athena_database_name'],
                                    athena_table_name=sample['athena_table_name'])

    # When
    result = gm.validate(batch_list=batch_list, suite=suite_one_expectation_athena)

    # Then
    assert result['success']
    for r in result['results']:
        pprint(r['result'])


# TODO: checkpoint action config 할 것
# TODO: KST 적용하기
# TODO: 산출물 다 지우기
def test_checkpoint_s3(gm, sample, suite_two_expectation):
    # When
    result = gm.checkpoint(s3_file_name=sample['s3_asset_name'], suite=suite_two_expectation)

    # Then
    assert not result.success
    print(result)
    # TODO: S3 경로 추적하고 지워야함


def test_checkpoint_athena(gm, sample, suite_one_expectation_athena):
    # When
    result = gm.checkpoint(athena_database_name=sample['athena_database_name'],
                           athena_table_name=sample['athena_table_name'],
                           suite=suite_one_expectation_athena)

    # Then
    assert result.success
    print(result)


def test_profile(gm, sample, suite_profile):
    # When
    profile_suite = gm.profile(s3_file_name=sample['s3_asset_name'], suite=suite_profile)

    # Then
    assert profile_suite
    print(f'suite={profile_suite}')


# TODO: metric store 체크하기
# TODO: 거버너 기준 코드 스니펫 작성하기
# s3 데이터 읽어서 프로파일 자동으로 suite 만들기
# s3 데이터 읽어서 expectation 추가해서 suite 만들기

# def test_spark(gm, sample, suite_one_expectation):
#     data_asset = gm.spark_datasource.add_csv_asset(
#             name=sample['s3_asset_name'],
#             s3_prefix=gm.DATA_S3_BUCKET_PREFIX
#         )
#     batch_request = data_asset.build_batch_request()
#     batch_list = gm.context.get_batch_list(batch_request=batch_request)
#     assert len(batch_list) > 0
#     pass
