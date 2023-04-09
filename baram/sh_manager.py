import fire
import boto3
import gspread

from baram.log_manager import LogManager


class SHManager(object):
    def __init__(self):
        self.cli = boto3.client('securityhub')
        self.logger = LogManager.get_logger()

    def list_findings(self, workflow_status='NEW', record_status='ACTIVE', compare='EQUALS', max_result=100):
        findings = self.cli.get_findings(Filters={'WorkflowStatus': [{'Value': workflow_status, 'Comparison': compare}],
                                                  'RecordState': [{'Value': record_status, 'Comparison': compare}]},
                                         MaxResults=max_result)
        result = findings['Findings']
        while 'NextToken' in findings:
            findings = self.cli.get_findings(Filters={'WorkflowStatus':
                                                          [{'Value': workflow_status, 'Comparison': compare}],
                                                      'RecordState':
                                                          [{'Value': record_status, 'Comparison': compare}]},
                                             MaxResults=max_result, NextToken=findings['NextToken'])
            result += findings['Findings']
        return result

    def export_to_google_sheet(self, service_file: str):
        """

        :param service_file: json key file for google cloud authentication
        :return: export and write security hub findings to linked google sheet file
        """
        values = []
        headers = ['SchemaVersion', 'Id', 'ProductArn',	'ProductName', 'CompanyName', 'Region',	'GeneratorId',
                   'AwsAccountId', 'Types', 'CreatedAt', 'UpdatedAt', 'Severity', 'Title', 'Description',
                   'ProductFields', 'Resources', 'Compliance', 'WorkflowState', 'Workflow', 'RecordState',
                   'FindingProviderFields', 'FirstObservedAt', 'LastObservedAt', 'Remediation', 'SourceUrl', 'Sample',
                   'Confidence']
        values.append(headers)

        for finding in self.list_findings():
            finding = [str(finding.get(header, 'N/A')) for header in headers]
            values.append(finding)

        google_cloud_authorize = gspread.service_account(filename=service_file)
        worksheet = google_cloud_authorize.open('sh_findings').get_worksheet(0)
        worksheet.update('A1', values)


if __name__ == '__main__':
    fire.Fire(SHManager)
