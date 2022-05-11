from baram.lambda_manager import LambdaManager
from baram.s3_manager import S3Manager

if __name__ == '__main__':
    sm = S3Manager('sli-dst-security')
    lm = LambdaManager()
    lm.publish_lambda_layer('baram', 'sli-dst-security', sm)
