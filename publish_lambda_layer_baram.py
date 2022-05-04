from baram.s3_manager import S3Manager
from baram.kms_manager import KMSManager
from baram.lambda_manager import LambdaManager

if __name__ == '__main__':
    sm = S3Manager('sli-dst-security', KMSManager().get_kms_arn('s3-hydra01-kms', False))
    lm = LambdaManager()
    lm.publish_lambda_layer('baram', 'sli-dst-security', sm)
