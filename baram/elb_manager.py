import traceback

import boto3

from baram.log_manager import LogManager


class ELBManager(object):
    def __init__(self):
        self.cli = boto3.client('elbv2')
        self.logger = LogManager.get_logger()

    def describe_load_balancers(self):
        return self.cli.describe_load_balancers()['LoadBalancers']

    def delete_load_balacner(self, elb_arn: str = ''):
        try:
            self.cli.delete_load_balancer(LoadBalancerArn=elb_arn)
        except:
            print(traceback.format_exc())