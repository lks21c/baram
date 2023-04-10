import traceback

import boto3
from json import dumps

from baram.log_manager import LogManager


class EC2Manager(object):
    def __init__(self):
        self.cli = boto3.client('ec2')
        self.logger = LogManager.get_logger()

    def list_sgs(self) -> list:
        """
        Describes the specified security groups or all of your security groups.

        :return: SecurityGroups
        """
        try:
            return self.cli.describe_security_groups()['SecurityGroups']
        except:
            print(traceback.format_exc())
            return None

    def list_unused_sg_ids(self, description_filter: str = '', sm_domain_ids: list = None) -> set:
        """
        Describe useless and deletable security group ids.
        (including disused SageMaker domain related things)

        :param description_filter: String in 'Description'
        :param sm_domain_ids: DomainId of sagemaker
        :return: GroupIds
        """
        # Get security groups not related to anything in ec2
        sgs = self.list_sgs()
        try:
            valid_sg_ids = set([pair['sg_id'] for pair in self.list_vpc_sg_eni_subnets()])
            result = [sg['GroupId'] for sg in sgs
                      if sg['GroupId'] not in valid_sg_ids
                      or (description_filter in sg['Description']
                          and sum([domain_id in sg['Description'] for domain_id in sm_domain_ids]) == 0)]

            return set(result)
        except TypeError:
            return None

    def list_vpc_sg_eni_subnets(self) -> list:
        """
        Return the list of all pairs of {VpcId, GroupId, NetworkInterfaceId, SubnetId}.

        :return: The list of {vpc_id, security_group_id, eni_id, subnet_id}
        """
        result = []
        vpc_ids = [vpc['VpcId'] for vpc in self.list_vpcs() if vpc['State'] == 'available']

        try:
            for vpc_id in vpc_ids:
                specified_sg_ids = self.get_sg_ids_with_vpc_id(vpc_id=vpc_id)
                for sg_id in specified_sg_ids:
                    specified_enis = self.get_eni_with_sg_id(sg_id=sg_id)
                    pairs = {'vpc_id': vpc_id, 'sg_id': sg_id}
                    for eni in specified_enis:
                        pairs['eni_id'], pairs['subnet_id'] = eni['NetworkInterfaceId'], eni['SubnetId']
                    result.append(pairs)
            return result

        except TypeError:
            print(traceback.format_exc())
            return None

    def get_default_vpc(self) -> list:
        """
        Get default vpc

        :return: Default Vpc
        """
        return [vpc for vpc in self.list_vpcs() if vpc['IsDefault']][0]

    def get_sg_ids_with_vpc_id(self, vpc_id: str) -> list:
        """
        Get security group ids of specific vpc.

        :param vpc_id: VpcId
        :return: GroupId
        """
        sgs = self.cli.describe_security_groups()['SecurityGroups']
        try:
            return [sg['GroupId'] for sg in sgs if sg['VpcId'] == vpc_id]
        except TypeError:
            print(traceback.format_exc())
            return None

    def get_eni_with_sg_id(self, sg_id: str) -> list:
        """
        Get network interfaces with specific security group.

        :param sg_id: GroupId
        :return: NetworkInterfaces
        """
        enis = self.cli.describe_network_interfaces()['NetworkInterfaces']
        try:
            return [eni for eni in enis if eni['Groups'] != [] and sg_id in [x['GroupId'] for x in eni['Groups']]]
        except TypeError:
            print(traceback.format_exc())
            return None

    def list_sg_relations(self) -> list:
        """
        Describe security groups with related security groups, including egress and ingress status.

        :return: SecurityGroupId, IsEgress, RelatedSecurityGroupId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        sg_ids = [sg_rule['GroupId'] for sg_rule in sg_rules]
        is_egress_list = [sg_rule['IsEgress'] for sg_rule in sg_rules]
        referenced_sgs = [x['ReferencedGroupInfo']['GroupId']
                          if 'ReferencedGroupInfo' in x.keys() else None for x in sg_rules]

        result = [{'sg_id': sg_ids[i],
                   'is_egress': is_egress_list[i],
                   'related_sg_id': referenced_sgs[i]} for i in range(len(sg_rules))]
        return result

    def get_related_sgs(self, sg_id: str) -> set:
        """
        Get the set of related security groups for specific security group.

        :param sg_id: GroupId
        :return: GroupId
        """
        sg_relations = self.list_sg_relations()
        return set([sg_relation['sg_id'] for sg_relation in sg_relations if sg_relation['related_sg_id'] == sg_id])

    def get_sg_rules(self, sg_id: str) -> list:
        """
        Get security group rules of specific security group.

        :param sg_id: GroupId
        :return: SecurityGroupRuleId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        result = [{'sg_rule_id': sg_rule['SecurityGroupRuleId'],
                   'is_egress': True if sg_rule['IsEgress'] else False}
                  for sg_rule in sg_rules if sg_rule['GroupId'] == sg_id]
        return result

    def delete_sg_rules(self, sg_id: str):
        """
        Get rid of security group rule of specific security group.

        :param sg_id: GroupId
        """
        sg_rules = self.get_sg_rules(sg_id)

        try:
            for sg_rule in sg_rules:
                if sg_rule['is_egress']:
                    self.cli.revoke_security_group_egress(GroupId=sg_id,
                                                          SecurityGroupRuleIds=[sg_rule['sg_rule_id']])
                else:
                    self.cli.revoke_security_group_ingress(GroupId=sg_id,
                                                           SecurityGroupRuleIds=[sg_rule['sg_rule_id']])
                self.logger.info('security group rule has deleted')
        except:
            print(traceback.format_exc())

    def delete_sgs(self, sg_ids: list):
        """
        Delete useless and deletable security groups.

        :param sg_ids: List of GroupId.
        :return:
        """
        try:
            for sg_id in sg_ids:
                self.delete_sg_rules(sg_id)
                self.delete_sg(sg_id)
        except:
            print(traceback.format_exc())

    def delete_sg(self, sg_id: str):
        """
        Delete security group.

        :param sg_id: GroupId
        """
        try:
            self.cli.delete_sg(GroupId=sg_id)
            self.logger.info('security group has deleted')
        except:
            print(traceback.format_exc())

    def list_instances_with_status(self, status: str = 'running'):
        """
        Describes all instances in specific status (ex: 'running', ...)

        :return: Instances, with status
        """
        try:
            instances = [instance['Instances'][0] for instance in self.describe_instances()['Reservations']]
            return [instance for instance in instances if instance['State']['Name'] == status]
        except:
            print(traceback.format_exc())
            return None

    def delete_unused_key_pairs(self):
        """
        Delete redundant key pairs (i.e. not related to any instances)
        """
        key_pairs_redundant = self.list_unused_key_pairs()
        try:
            for key_pair in key_pairs_redundant:
                self.cli.delete_key_pair(KeyName=key_pair)
                self.logger.info('key pair has deleted')
        except:
            print(traceback.format_exc())

    def list_unused_key_pairs(self):
        """
        Describes all disused key pairs

        :return: KeyName
        """
        key_pairs_total = self.list_key_pairs()
        instances = [instance['Instances'][0] for instance in self.describe_instances()['Reservations']]
        key_pairs_using = [instance['KeyName'] for instance in instances if 'KeyName' in instance]

        return key_pairs_total - set(key_pairs_using)

    def list_key_pairs(self):
        """
        Describes all key pairs

        :return: KeyName
        """
        key_pairs = self.cli.describe_key_pairs()['KeyPairs']
        return set([key_pair['KeyName'] for key_pair in key_pairs])

    def list_vpcs(self) -> list:
        """
        List one or more of your VPCs.

        :return: Vpcs
        """
        return self.cli.describe_vpcs()['Vpcs']

    def list_detail_vpcs(self) -> list:
        """
        List one or more of your VPCs with detail.

        :return: Vpcs.
        """
        vpc_list = []
        for vpc in self.list_vpcs():
            name = ''
            if 'Tags' in vpc:
                name = next(t['Value'] for t in vpc['Tags'] if t['Key'] == 'Name')
            elif vpc['IsDefault']:
                name = 'default'
            vpc_list.append(
                {'vpc_id': vpc['VpcId'],
                 'name': name,
                 'cidr_block': vpc['CidrBlock'],
                 'state': vpc['State']})
        return vpc_list

    def list_subnets(self) -> list:
        """
        List one or more of your Subnets.

        :return: Subnets
        """
        return self.cli.describe_subnets()['Subnets']

    def list_detail_subnets(self) -> list:
        """
        List one or more of your Subnets with detail.

        :return: Subnets
        """
        subnet_list = []
        for subnet in self.list_subnets():
            name = ''
            vpc_name = ''
            if 'Tags' in subnet:
                name = [t['Value'] for t in subnet['Tags'] if t['Key'] == 'Name']
                vpc_name = [t['Value'] for t in subnet['Tags'] if t['Key'] == 'aws:cloudformation:stack-name']

            subnet_list.append(
                {'subnet_id': subnet['SubnetId'],
                 'vpc_id': subnet['VpcId'],
                 'name': '' if len(name) == 0 else name[0],
                 'vpc_name': '' if len(vpc_name) == 0 else vpc_name[0],
                 'cidr_block': subnet['CidrBlock'],
                 'state': subnet['State']})
        return subnet_list

    def get_subnet_ids_with_vpc_id(self, vpc_id: str) -> list:
        """
        Retrieve subnet ids attached to specified VPC id
        :param vpc_id: vpc id
        :return:
        """
        return [i['SubnetId'] for i in self.list_subnets() if i['VpcId'] == vpc_id]

    def list_enis(self) -> list:
        """
        List one or more of your Network Interfaces.

        :return:
        """
        return self.cli.describe_network_interfaces()['NetworkInterfaces']

    def list_tgws(self) -> list:
        """
        Get Id of Transit Gateway

        :return: TransitGateways
        """
        return self.cli.describe_transit_gateways()['TransitGateways']

    def get_sg_id_with_sg_name(self, group_name: str) -> str:
        """
        Retrieve subnet id from group name.

        :param group_name: GroupName
        :return: GroupId
        """
        return next(
            (i['GroupId'] for i in self.cli.describe_security_groups()['SecurityGroups']
             if group_name.lower() in i['GroupName'].lower()), None)

    def get_vpc_id_with_vpc_name(self, vpc_name: str) -> str:
        """
        Retrieve vpc id from vpc name

        :param vpc_name: vpc name
        :return:
        """
        return next(i['VpcId'] for i in self.list_vpcs() if 'Tags' in i
                    for t in i['Tags'] if vpc_name.lower() in t['Value'].lower())

    def get_subnet_id(self, vpc_id: str, subnet_name: str) -> str:
        """
        Retrieve subnet id from vpc id and subnet name.

        :param vpc_id: vpc_id
        :param subnet_name: subnet_name
        :return:
        """
        return next(s['SubnetId'] for s in self.list_subnets() if vpc_id == s['VpcId'] and 'Tags' in s
                    for t in s['Tags'] if subnet_name == t['Value'])

    def get_ec2_id_with_ec2_name(self, ec2_name: str) -> str:
        """

        :param ec2_name: ec2 instance name
        :return:
        """
        ec2 = boto3.resource('ec2')
        return next(
            i.id for i in ec2.instances.all() if i.state['Name'] == 'running' for t in i.tags if ec2_name == t['Value'])

    def describe_instances(self, instance_id_list: list = None) -> list:
        """

        Retrieve ec2 instance description.
        :param instance_id_list: ec2 instance id list
        :return:
        """
        if instance_id_list is not None:
            return self.cli.describe_instances(InstanceIds=instance_id_list)
        else:
            return self.cli.describe_instances()

    def get_ec2_instances_with_imds_v1(self) -> list:
        """

        :return: get ec2 instances that support imds_v1.
        """
        response = self.describe_instances()
        return [i['InstanceId'] for r in response['Reservations']
                for i in r['Instances']
                if i['MetadataOptions']['HttpTokens'] != 'required' and i['State']['Name'] == 'running']

    def apply_imdsv2_only_mode(self,
                               instances_list: list = None,
                               http_put_response_hop_limit: int = 1):
        """

        Apply imdsv2 only mode into ec2 instances.
        :param instances_list:
        :param http_put_response_hop_limit: see https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceMetadataOptionsRequest.html.
        :return:
        """
        for i in instances_list:
            self.cli.modify_instance_metadata_options(InstanceId=i,
                                                      HttpTokens='required',
                                                      HttpPutResponseHopLimit=http_put_response_hop_limit,
                                                      HttpEndpoint='enabled')

    def describe_route_tables(self):
        return self.cli.describe_route_tables()['RouteTables']

    def delete_route(self, destination_cidr_block: str = '', route_table_id: str = ''):
        """

        :param destination_cidr_block:
        :param route_table_id:
        """
        try:
            self.cli.delete_route(DestinationCidrBlock=destination_cidr_block, RouteTableId=route_table_id)
            self.logger.info(f'Route has deleted')
        except:
            print(traceback.format_exc())

    def delete_internet_gateway(self, igw_id: str = '', vpc_id: str = ''):
        """
        Delete internet gateway with its ID

        :param igw_id:
        """
        all_igw_ids = [igw['InternetGatewayId'] for igw in self.cli.describe_internet_gateways()['InternetGateways']]
        try:
            self.cli.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
            self.cli.delete_internet_gateway(InternetGatewayId=igw_id)
            is_deleted = igw_id in all_igw_ids
            while is_deleted:
                all_igw_ids = [igw['InternetGatewayId']
                               for igw in self.cli.describe_internet_gateways()['InternetGateways']]
                is_deleted = igw_id in all_igw_ids
            self.logger.info(f'Internet gateway {igw_id} has deleted')
        except:
            print(traceback.format_exc())


    def delete_nat_gateway(self, nat_gw_id: str = ''):
        """
        Delete nat gateway with its ID

        :param nat_gw_id: NatGatewayId
        """
        all_nat_gws = self.cli.describe_nat_gateways()['NatGateways']
        nat_gw_info = [nat_gw for nat_gw in all_nat_gws if nat_gw['NatGatewayId'] == nat_gw_id]
        if len(nat_gw_info) > 0:
            try:
                self.cli.delete_nat_gateway(NatGatewayId=nat_gw_id)
                while nat_gw_info[0]['State'] != 'deleted':
                    all_nat_gws = self.cli.describe_nat_gateways()['NatGateways']
                    nat_gw_info = [nat_gw for nat_gw in all_nat_gws if nat_gw['NatGatewayId'] == nat_gw_id]
                self.logger.info(f'Nat gateway {nat_gw_id} has deleted')
            except:
                print(traceback.format_exc())

    def delete_endpoints(self, endpoint_ids: list = []):
        """
        Delete vpc endpoints with their id

        :param endpoint_ids:
        """
        all_endpoint_ids = [ep['VpcEndpointId'] for ep in self.cli.describe_vpc_endpoints()['VpcEndpoints']]
        try:
            self.cli.delete_vpc_endpoints(VpcEndpointIds=endpoint_ids)
            while sum([ep_id in all_endpoint_ids for ep_id in endpoint_ids]) != 0:
                all_endpoint_ids = [ep['VpcEndpointId'] for ep in self.cli.describe_vpc_endpoints()['VpcEndpoints']]
            for ep_id in endpoint_ids:
                self.logger.info(f'Endpoint {ep_id} has deleted')
        except:
            print(traceback.format_exc())

    def delete_vpc(self, vpc_id: str = ''):
        """
        Delete vpc with its ID.

        :param vpc_id: VpcId
        """
        # route_tables = [rtb for rtb in self.describe_route_tables() if rtb['VpcId'] == vpc_id]
        # route_dumps = [rtb['RouteTableId'] for rtb in route_tables if 'igw' in dumps(rtb['Routes'])]

        # all_igws = self.cli.describe_internet_gateways()['InternetGateways']
        # igw_ids = [igw['InternetGatewayId'] for igw in all_igws if vpc_id in igw['Attachments'][0]['VpcId']]

        all_nat_gws = self.cli.describe_nat_gateways()['NatGateways']
        nat_gw_ids = [nat_gw['NatGatewayId'] for nat_gw in all_nat_gws if vpc_id in nat_gw['VpcId']]

        all_endpoints = self.cli.describe_vpc_endpoints()['VpcEndpoints']
        endpoint_ids = [ep['VpcEndpointId'] for ep in all_endpoints if vpc_id in ep['VpcId']]

        # if len(igw_ids) > 0:
        #     for igw_id in igw_ids:
        #         igw_route_table = [rtb for rtb in route_tables if 'igw' in dumps(rtb['Routes'])]
        #         if len(igw_route_table) > 0:
        #             igw_route_table_id = igw_route_table['RouteTableId']
        #             igw_route = [rt for rt in igw_route_table['Routes'] if 'igw' in dumps(rt)][0]
        #
        #             print(igw_route_table_id)
        #             print(igw_route)
        #             self.delete_route(igw_route['DestinationCidrBlock'], igw_route_table_id)
        #         self.delete_internet_gateway(igw_id, vpc_id)

        if len(nat_gw_ids) > 0:
            for nat_gw_id in nat_gw_ids:
                self.delete_nat_gateway(nat_gw_id)

        if len(endpoint_ids) > 0:
            self.delete_endpoints(endpoint_ids)

        try:
            self.cli.delete_vpc(VpcId=vpc_id)
            vpc_ids = [vpc['VpcId'] for vpc in self.list_vpcs()]
            while vpc_id in vpc_ids:
                vpc_ids = [vpc['VpcId'] for vpc in self.list_vpcs()]
            self.logger.info(f'VPC {vpc_id} has deleted')
        except:
            print(traceback.format_exc())
