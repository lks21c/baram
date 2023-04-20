from typing import List


class CIDRBlockState:
    state: str

    def __init__(self, state: str) -> None:
        self.state = state


class CIDRBlockAssociationSet:
    association_id: str
    cidr_block: str
    cidr_block_state: CIDRBlockState

    def __init__(self, association_id: str, cidr_block: str, cidr_block_state: CIDRBlockState) -> None:
        self.association_id = association_id
        self.cidr_block = cidr_block
        self.cidr_block_state = cidr_block_state


class Tag:
    key: str
    value: str

    def __init__(self, key: str, value: str) -> None:
        self.key = key
        self.value = value


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

    def __init__(self, cidr_block: str, dhcp_options_id: str, state: str, vpc_id: str, owner_id: str, instance_tenancy: str, cidr_block_association_set: List[CIDRBlockAssociationSet], is_default: bool, tags: List[Tag]) -> None:
        self.cidr_block = cidr_block
        self.dhcp_options_id = dhcp_options_id
        self.state = state
        self.vpc_id = vpc_id
        self.owner_id = owner_id
        self.instance_tenancy = instance_tenancy
        self.cidr_block_association_set = cidr_block_association_set
        self.is_default = is_default
        self.tags = tags


class Vpcs:
    vpcs: List[Vpc]

    def __init__(self, vpcs: List[Vpc]) -> None:
        self.vpcs = vpcs
