import os

import boto3

from baram.log_manager import LogManager
from baram.process_manager import ProcessManager
from baram.s3_manager import S3Manager


class LambdaManager(object):
    def __init__(self):
        self.cli = boto3.client('lambda')
        self.logger = LogManager.get_logger()

    def list_layers(self):
        '''
        Lists Lambda layers and shows information about the latest version of each.

        :return: list layers
        '''
        return self.cli.list_layers()['Layers']

    def get_latest_layer_arn(self, layer_name):
        '''
        Retrieve latest layer arn from layer name.

        :param layer_name: layer name
        :return: baram latest layer ARN.
        '''
        return [l['LatestMatchingVersion']['LayerVersionArn']
                for l in self.list_layers() if l['LayerName'] == layer_name][0]

    def publish_lambda_layer(self, layer_name, s3_bucket_name: str, sm: S3Manager):
        '''
        publish lamber layer from local wheel.

        :param layer_name: layer name
        :param s3_bucket_name: s3 bucket name
        :param sm: S3Manager instance
        :return:
        '''
        cmd = f'rm -rf python ' \
              f"&& mkdir python " \
              f'&& cd python && pip3 install {layer_name} -t . ' \
              f'&& cd .. ' \
              f'&& zip -r {layer_name}.zip python ' \
              f'&& rm -rf python'
        self.logger.info(cmd)
        ProcessManager.run_cmd(cmd, False)

        sm.upload_file(os.path.join(os.getcwd(), f'{layer_name}.zip'), f'lambda_layers/{layer_name}.zip')
        os.remove(f'{layer_name}.zip')

        content = {
            'S3Bucket': s3_bucket_name,
            'S3Key': f'lambda_layers/{layer_name}.zip'
        }
        self.logger.info(content['S3Key'])

        arn = ':'.join(self.get_latest_layer_arn(layer_name).split(":")[:-1])
        self.cli.publish_layer_version(LayerName=arn,
                                       Content=content,
                                       CompatibleRuntimes=['python3.7', 'python3.8', 'python3.9'])
