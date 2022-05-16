import boto3


class SagemakerManager(object):
    def __init__(self):
        self.cli = boto3.client('sagemaker')

    def describe_image(self, name: str):
        return self.cli.describe_image(ImageName=name)

    def describe_image_version(self, name: str):
        response = self.cli.describe_image_version(ImageName=name)
        return response

    def create_image_version(self, image_uri: str, name: str):
        return self.cli.create_image_version(
            BaseImage=image_uri,
            ImageName=name
        )
