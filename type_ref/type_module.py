import json
from typing import Any, TypedDict

from botocore.client import BaseClient
from pprint import pprint


class IdList(TypedDict):
    eni_id: str
    vpc_id: str
    sg_id: str
    subnet_id: str

class IdLists:
    id_lists: list[IdList]

class TypeCheck:
    def print_return_type(value):
        print(f"Type : {type(value)}")
        # js = json.dumps(value)
        # # print(type(js))
        # # print(js)
        # js_v = json.loads(js)
        pprint(value)

    def type_test(self):
        ts = [{'eni_id': 'eni-04a1b78149f92fb0b',
               'sg_id': 'sg-083c9f868ca8d0129',
               'subnet_id': 'subnet-0491719e14197fbdf',
               'vpc_id': 'vpc-0116a6c58654c68af'}]
        assert ts == list
        assert ts == list[dict[str, Any]]
        assert ts == list[dict[str, ...]]
