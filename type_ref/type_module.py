from __future__ import annotations

from enum import Enum
from typing import Any, TypedDict, Union, Optional



# EC2
class IdList(TypedDict):
    eni_id: str
    vpc_id: str
    sg_id: str
    subnet_id: str


class IdLists(TypedDict):
    id_lists: list[IdList]


# Glue
class GlueCommand(TypedDict):
    Name: str
    ScriptLocation: bytes
    PythonVersion: str


class GlueTable(TypedDict):
    DatabaseName: str
    Name: str


# S3
class PutObjectParam(TypedDict):
    Bucket: str
    Key: str
    Body: Union[bytes, str]
    ServerSideEncryption: str = None
    SSEKMSKeyId: str = None


# sagemaker
class AppType(Enum):
    JupyterServer = 'JupyterServer'
    KernelGateway = 'KernelGateway'
    TensorBoard = 'TensorBoard'
    RStudioServerPro = 'RStudioServerPro'
    RSessionGateway = ' RSessionGateway'
