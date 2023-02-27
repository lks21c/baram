import boto3
import fire
import pandas as pd
import pygsheets

from baram.log_manager import LogManager


class SHManager(object):
    def __init__(self):
        self.cli = boto3.client('securityhub')
        self.logger = LogManager.get_logger()

    def list_findings(self, status='NEW', compare='EQUALS', max_result=100):
        findings = self.cli.get_findings(Filters={'WorkflowStatus': [{'Value': status, 'Comparison': compare}]},
                                         MaxResults=max_result)
        result = [i for i in findings['Findings']]
        next_token = findings['NextToken']
        while True:
            next_findings = self.cli.get_findings(Filters={'WorkflowStatus': [{'Value': status, 'Comparison': compare}]},
                                                  MaxResults=max_result, NextToken=next_token)
            result += next_findings['Findings']
            if 'NextToken' in next_findings:
                next_token = findings['NextToken']
            else:
                break
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
