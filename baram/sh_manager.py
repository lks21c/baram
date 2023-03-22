import fire
import boto3

import pygsheets
import pandas as pd

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

        :param service_file: json key file for authentication
        :return: export and write sh_file findings to google sheet
        """
        google_cloud_authorize = pygsheets.authorize(service_file=service_file)
        sh_findings = self.list_findings()
        assert len(sh_findings) > 0, 'there is no security hub findings'

        try:
            sh_findings_df = pd.DataFrame(sh_findings)
            sh_file = google_cloud_authorize.open('sh_findings')
            worksheet = sh_file[0]
            worksheet.set_dataframe(sh_findings_df, (1, 1))
        except:
            self.logger.info('error')


if __name__ == '__main__':
    fire.Fire(SHManager)
