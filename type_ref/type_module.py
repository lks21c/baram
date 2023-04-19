from botocore.client import BaseClient

# type alias
BotoClient = BaseClient


class TypeCheck:
    def check_type_returnValue(value):
        print(f"Type : {type(value)}")
        print(f"Value : {value}")


