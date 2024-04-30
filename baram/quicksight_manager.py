from datetime import datetime
from typing import Union, Type

import boto3
import tzlocal


class QuicksightManager(object):
    def __init__(self):
        self.cli = boto3.client('quicksight')
        self.TIMEZONE = tzlocal.get_localzone().key

    def describe_dataset_refresh_properties(self, account_id: str, dataset_id: str):
        '''
        Describes the refresh properties of a specific QuickSight dataset

        :param account_id:
        :param dataset_id:
        :return:
        '''
        return self.cli.describe_data_set_refresh_properties(AwsAccountId=account_id, DataSetId=dataset_id)

    def list_datasets(self, account_id: str, max_results: int = 100):
        '''
        List QuickSight datasets of specific account ID.

        :param account_id: AWS account ID
        :param max_results: The maximum number of results to be returned
        :return: Dataset lists
        '''
        return self.cli.list_data_sets(AwsAccountId=account_id, MaxResults=max_results)

    def get_dataset_arns(self, account_id: str, max_results: int = 100):
        '''
        Get the list of ARNs for every QuickSight datasets.

        :param account_id: AWS account ID
        :param max_results: The maximum number of results to be returned
        :return: Dataset ARN lists
        '''
        datasets = self.list_datasets(account_id, max_results)
        return [dataset['Arn'] for dataset in datasets['DataSetSummaries']]

    def get_dataset_arn_with_name(self, account_id: str, dataset_name: str, max_results: int = 100):
        '''
        Get the ARN of specific QuickSight dataset via its name.

        :param account_id: AWS account ID
        :param dataset_name: The name of specific QuickSight dataset
        :param max_results: The maximum number of results to be returned
        :return: An ARN of specific dataset
        '''
        datasets = self.list_datasets(account_id, max_results)
        return [dataset['Arn'] for dataset in datasets['DataSetSummaries'] if dataset['Name'] == dataset_name][0]

    def get_dataset_arn_with_id(self, account_id: str, dataset_id: str, max_results: int = 100):
        '''
        Get the arn of specific QuickSight dataset via its id.

        :param account_id: AWS account ID
        :param dataset_id: ID of QuickSight dataset
        :param max_results: The maximum number of results to be returned
        :return: An ARN of specific dataset
        '''
        datasets = self.list_datasets(account_id, max_results)
        return [dataset['Arn'] for dataset in datasets['DataSetSummaries'] if dataset['DataSetId'] == dataset_id][0]

    def get_dataset_ids(self, account_id: str, max_results: int = 100):
        '''
        Get the list of IDs for every QuickSight datasets.

        :param account_id: aws account ID
        :param max_results: the maximum number of results to be returned
        :return: Dataset ID lists
        '''
        datasets = self.list_datasets(account_id, max_results)

        return [dataset['DataSetId'] for dataset in datasets['DataSetSummaries']]

    def get_dataset_id_with_arn(self, account_id: str, arn: str, max_results: int = 100):
        '''
        Get ID of QuickSight dataset via its arn

        :param account_id: AWS account ID
        :param arn: ARN of QuickSight dataset
        :param max_results: The maximum number of results to be returned
        :return:
        '''
        datasets = self.list_datasets(account_id, max_results)
        return [dataset['DataSetId'] for dataset in datasets['DataSetSummaries'] if dataset['Arn'] == arn][0]

    def get_dataset_id_with_name(self, account_id: str, dataset_name: str, max_results: int = 100):
        '''
        Get ID of QuickSight dataset via its arn

        :param account_id: AWS account ID
        :param dataset_name: The name of specific QuickSight dataset
        :param max_results: The maximum number of results to be returned
        :return:
        '''
        datasets = self.list_datasets(account_id, max_results)
        return [dataset['DataSetId'] for dataset in datasets['DataSetSummaries'] if dataset['Name'] == dataset_name][0]

    def create_dataset_refresh_schedule(self,
                                        account_id: str,
                                        dataset_id: str,
                                        schedule_id: str,
                                        schedule_interval: Union['minute15', 'minute30', 'hourly',
                                                                 'daily', 'weekly', 'monthly'],
                                        time_of_the_day: str = '',
                                        timezone: str = None,
                                        refresh_day_weekly: Union[None, 'sun', 'mon', 'tue',
                                                                  'wed', 'thur', 'fri', 'sat'] = None,
                                        refresh_day_month: Union[None, Type[str], Type[int]] = None,
                                        refresh_type: str = 'full',
                                        start_after_datetime: datetime = None):
        '''

        :param account_id:
        :param dataset_id:
        :param schedule_id:
        :param schedule_interval:
        :param time_of_the_day:
        :param timezone:
        :param refresh_day_weekly:
        :param refresh_day_month:
        :param refresh_type:
        :param start_after_datetime:
        :return:
        '''
        if schedule_interval == 'hourly':
            assert time_of_the_day != ''

        schedule = {'ScheduleId': schedule_id,
                    'ScheduleFrequency': {
                        'Interval': schedule_interval.upper(),
                        'Timezone': timezone if timezone else self.TIMEZONE,
                        'TimeOfTheDay': time_of_the_day
                    },
                    'StartAfterDateTime': start_after_datetime if start_after_datetime else datetime.now(),
                    'RefreshType': f"{refresh_type}_refresh".upper()}

        if refresh_day_weekly:
            assert schedule_interval == 'weekly'
            day = f'{refresh_day_weekly}sday' if refresh_day_weekly in ['tue', 'thur'] \
                else f'{refresh_day_weekly}nesday' if refresh_day_weekly == 'wed' \
                else f'{refresh_day_weekly}urday' if refresh_day_weekly == 'sat' \
                else f'{refresh_day_weekly}day'
            schedule['ScheduleFrequency']['RefreshOnDay'] = {'DayOfWeek': day.upper()}
        if refresh_day_month:
            assert schedule_interval == 'monthly'
            schedule['ScheduleFrequency']['RefreshOnDay'] = {'DayOfMonth': f"{refresh_day_month}"}

        return self.cli.create_refresh_schedule(DataSetId=dataset_id,
                                                AwsAccountId=account_id,
                                                Schedule=schedule)

    def delete_dataset_refresh_schedule(self, account_id: str, dataset_id: str, schedule_id: str):
        '''

        :param account_id:
        :param dataset_id:
        :param schedule_id:
        :return:
        '''
        return self.cli.delete_refresh_schedule(AwsAccountId=account_id,
                                                DataSetId=dataset_id,
                                                ScheduleId=schedule_id)

    def delete_dataset_refresh_schedules(self, account_id: str, dataset_id: str):
        '''

        :param account_id:
        :param dataset_id:
        :param schedule_id:
        :return:
        '''
        schedules = self.list_refresh_schedules(account_id, dataset_id)
        if len(schedules) > 0:
            for schedule_id in [x['ScheduleId'] for x in schedules]:
                self.cli.delete_refresh_schedule(AwsAccountId=account_id,
                                                 DataSetId=dataset_id,
                                                 ScheduleId=schedule_id)
        else:
            pass

    def list_dataset_refresh_schedules(self, account_id: str, dataset_id: str):
        '''
        List refresh schedule for a specific QuickSight dataset

        :param account_id: aws account ID
        :param dataset_id: ID of QuickSight dataset
        :return:
        '''
        return self.cli.list_refresh_schedules(AwsAccountId=account_id,
                                               DataSetId=dataset_id)['RefreshSchedules']

    def update_dataset_refresh_schedule(self, account_id: str, dataset_id: str):
        '''
        Update refresh schedule for a specific QuickSight dataset

        :param account_id:
        :param dataset_id:
        :return:
        '''
        pass
