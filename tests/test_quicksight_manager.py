from pprint import pprint

import pytest

from baram.quicksight_manager import QuicksightManager


@pytest.fixture()
def qm():
    return QuicksightManager()


# def test_describe_data_set_refresh_properties(qm):
#     # Given
#     account_id = '145885190059'
#     dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
#
#     # When
#     result = qm.describe_dataset_refresh_properties(account_id, dataset_id)
#
#     # Then
#     pprint(result)


def test_list_datasets(qm):
    # Given
    account_id = '145885190059'

    # When
    datasets = qm.list_datasets(account_id)['DataSetSummaries']
    pprint(datasets)

    # Then
    arns = [dataset['Arn'] for dataset in datasets]
    assert len(arns) == sum([True if account_id in arn else False for arn in arns])


def test_get_dataset_arns(qm):
    # Given
    account_id = '145885190059'

    # When
    arns = qm.get_dataset_arns(account_id)
    pprint(arns)

    # Then
    assert len(arns) == sum([True if account_id in arn else False for arn in arns])


def test_get_dataset_arn_with_name(qm):
    # Given
    account_id = '145885190059'
    dataset_name = 'People Overview'

    # When
    arn = qm.get_dataset_arn_with_name(account_id, dataset_name)

    # Then
    assert len(arn.split(':')) == 6
    assert account_id in arn
    assert arn == 'arn:aws:quicksight:ap-northeast-2:145885190059:dataset/011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'


def test_get_dataset_arn_with_id(qm):
    # Given
    account_id = '145885190059'
    dataset_id = '011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'

    # When
    arn = qm.get_dataset_arn_with_id(account_id, dataset_id)

    # Then
    assert len(arn.split(':')) == 6
    assert account_id in arn
    assert dataset_id in arn
    assert arn == 'arn:aws:quicksight:ap-northeast-2:145885190059:dataset/011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'


def test_get_dataset_ids(qm):
    # Given
    account_id = '145885190059'

    # When
    dataset_ids = qm.get_dataset_ids(account_id)
    pprint(dataset_ids)

    # Then
    for dataset_id in dataset_ids:
        assert len(dataset_id.split('-')) == 5


def test_get_dataset_id_with_arn(qm):
    # Given
    account_id = '145885190059'
    arn = 'arn:aws:quicksight:ap-northeast-2:145885190059:dataset/011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'

    # When
    dataset_id = qm.get_dataset_id_with_arn(account_id, arn)

    # Then
    assert len(dataset_id.split('-')) == 5
    assert dataset_id == '011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'


def test_get_dataset_id_with_name(qm):
    # Given
    account_id = '145885190059'
    dataset_name = 'People Overview'

    # When
    dataset_id = qm.get_dataset_id_with_name(account_id, dataset_name)

    # Then
    assert len(dataset_id.split('-')) == 5
    assert dataset_id == '011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'

#
# def test_create_dataset_refresh_schedule_hourly(qm):
#     # Given
#     account_id = '145885190059'
#     dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
#     schedule_interval = 'hourly'
#     schedule_id = f'iu001_test_{schedule_interval}'
#     refresh_type = 'full'
#
#     # When
#     response = qm.create_dataset_refresh_schedule(account_id=account_id,
#                                                   dataset_id=dataset_id,
#                                                   schedule_id=schedule_id,
#                                                   schedule_interval=schedule_interval,
#                                                   refresh_type=refresh_type)
#
#     print(response)
#     # TODO: Add assertion
#
#
# def test_create_dataset_refresh_schedule_daily(qm):
#     # Given
#     account_id = '145885190059'
#     dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
#     schedule_interval = 'daily'
#     schedule_id = f'iu001_test_{schedule_interval}'
#     timeset = datetime.now() + timedelta(minutes=5)
#     time_of_the_day = f"{timeset.hour}:{timeset.minute}"
#     refresh_type = 'full'
#
#     # When
#     response = qm.create_dataset_refresh_schedule(account_id=account_id,
#                                                   dataset_id=dataset_id,
#                                                   schedule_id=schedule_id,
#                                                   schedule_interval=schedule_interval,
#                                                   time_of_the_day=time_of_the_day,
#                                                   refresh_type=refresh_type)
#
#     print(response)
#     # TODO: Add assertion
#
#
# def test_create_dataset_refresh_schedule_weekly(qm):
#     # Given
#     account_id = '145885190059'
#     dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
#     schedule_interval = 'weekly'
#     schedule_id = f'iu001_test_{schedule_interval}'
#     timeset = datetime.now() + timedelta(minutes=5)
#     time_of_the_day = f"{timeset.hour}:{timeset.minute}"
#     refresh_day_weekly = 'sat'
#     refresh_type = 'full'
#
#     # When
#     response = qm.create_dataset_refresh_schedule(account_id=account_id,
#                                                   dataset_id=dataset_id,
#                                                   schedule_id=schedule_id,
#                                                   schedule_interval=schedule_interval,
#                                                   time_of_the_day=time_of_the_day,
#                                                   refresh_day_weekly=refresh_day_weekly,
#                                                   refresh_type=refresh_type)
#
#     print(response)
#     # TODO: Add assertion
#
#
# def test_create_dataset_refresh_schedule_monthly(qm):
#     # Given
#     account_id = '145885190059'
#     dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
#     schedule_interval = 'monthly'
#     schedule_id = f'iu001_test_{schedule_interval}'
#     timeset = datetime.now() + timedelta(minutes=5)
#     time_of_the_day = f"{timeset.hour}:{timeset.minute}"
#     refresh_day_month = 3
#     refresh_type = 'full'
#
#     # When
#     response = qm.create_dataset_refresh_schedule(account_id=account_id,
#                                                   dataset_id=dataset_id,
#                                                   schedule_id=schedule_id,
#                                                   schedule_interval=schedule_interval,
#                                                   time_of_the_day=time_of_the_day,
#                                                   refresh_day_month=refresh_day_month,
#                                                   refresh_type=refresh_type)
#
#     print(response)
#     # TODO: Add assertion
#
#
# def test_delete_dataset_refresh_schedule(qm):
#     # TODO
#     assert False
#
#
# def test_delete_dataset_refresh_schedules(qm):
#     # TODO
#     assert False


def test_list_dataset_refresh_schedules(qm):
    # Given
    account_id = '145885190059'
    dataset_id = '3cf78960-2739-42d2-91ef-46395225dceb'  # iu001
    # dataset_id = '011f6910-ec66-4f3b-b3f1-e2f15ebdeabb'  # People Overview

    # When
    rf_schedules = qm.list_dataset_refresh_schedules(account_id, dataset_id)
    pprint(rf_schedules)

    # Then
    # TODO: Add assertion