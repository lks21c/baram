import boto3


class QuicksightManager(object):
    def __init__(self):
        self.cli = boto3.client('quicksight')

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

    def list_refresh_schedules(self, account_id: str, dataset_id: str):
        '''
        List refresh schedule for a specific QuickSight dataset

        :param account_id: aws account ID
        :param dataset_id: ID of QuickSight dataset
        :return:
        '''
        # return self.cli.list_refresh_schedules(AwsAccountId=account_id, DataSetId=dataset_id)
        return self.cli.describe_data_set_refresh_properties(AwsAccountId=account_id, DataSetId=dataset_id)



