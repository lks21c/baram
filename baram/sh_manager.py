import boto3
import fire
import pygsheets
import pandas as pd


class SHManager(object):
    def __init__(self):
        self.cli = boto3.client('securityhub')

    def list_findings(self):
        response = self.cli.get_findings(Filters={'WorkflowStatus': [{'Value': 'NEW', 'Comparison': 'EQUALS'}]},
                                         MaxResults=100)
        findings = response['Findings']
        return findings

    def export_to_gsheet(self, service_file: str):
        """
        :param service_file: json key file for authentication
        :return: export sh findings to google sheet
        """
        gc = pygsheets.authorize(service_file=service_file)
        df = pd.DataFrame(self.list_findings())
        sh = gc.open('sh_findings')
        wks = sh[0]
        wks.set_dataframe(df, (1, 1))


if __name__ == '__main__':
    fire.Fire(SHManager)
