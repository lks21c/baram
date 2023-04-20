from typing import List


class CIDRBlockState:
    state: str


class CIDRBlockAssociationSet:
    association_id: str
    cidr_block: str
    cidr_block_state: CIDRBlockState


class Tag:
    key: str
    value: str


class Vpc:
    cidr_block: str
    dhcp_options_id: str
    state: str
    vpc_id: str
    owner_id: str
    instance_tenancy: str
    cidr_block_association_set: List[CIDRBlockAssociationSet]
    is_default: bool
    tags: List[Tag]
