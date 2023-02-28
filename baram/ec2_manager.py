import boto3
import traceback

from baram.log_manager import LogManager


class EC2Manager(object):
    def __init__(self):
        self.cli = boto3.client('ec2')
        self.logger = LogManager.get_logger()

    def list_security_groups(self):
        """
        Describes the specified security groups or all of your security groups.

        :return: SecurityGroups
        """
        return self.cli.describe_security_groups()['SecurityGroups']

    def list_redundant_security_group_ids(self, redundant_domain_ids: list = None):
        """
        Describe useless and deletable security group ids.
        (including disused SageMaker domain related things)

        :param redundant_domain_ids:
        :return: GroupIds
        """
        # 1. Get security groups not related to anything in ec2
        vpc_sg_eni_subnets = self.list_vpc_sg_eni_subnets()
        security_group_valid_ids = set([pair['sg_id'] for pair in vpc_sg_eni_subnets])

        security_groups = self.list_security_groups()
        security_group_ids_total = set([sg['GroupId'] for sg in security_groups])

        result = security_group_ids_total - security_group_valid_ids

        # 2. Get security groups related to disused domains; SageMaker (NFS related)
        if redundant_domain_ids is not None:
            nfs_sgs = [sg['GroupId'] for sg in security_groups
                       if 'NFS' in sg['Description']
                       and sum([domain_id in sg['Description'] for domain_id in redundant_domain_ids]) == 0]
            result.union(nfs_sgs)

        return result

    def list_vpc_sg_eni_subnets(self):
        """
        Return the list of all pairs of {VpcId, GroupId, NetworkInterfaceId, SubnetId}.

        :return: The list of {vpc_id, security_group_id, eni_id, subnet_id}
        """
        vpcs = [vpc for vpc in self.list_vpcs() if vpc['State'] == 'available']
        vpc_ids = [vpc['VpcId'] for vpc in vpcs]

        result = []
        for vpc_id in vpc_ids:
            specified_sg_ids = self.get_sg_ids_with_vpc_id(vpc_id=vpc_id)
            for sg_id in specified_sg_ids:
                specified_enis = self.get_eni_with_sg_id(security_group_id=sg_id)
                pairs = {'vpc_id': vpc_id, 'sg_id': sg_id}
                if len(specified_enis) != 0:
                    for eni in specified_enis:
                        pairs['eni_id'] = eni['NetworkInterfaceId']
                        pairs['subnet_id'] = eni['SubnetId']
                result.append(pairs)

        return result

    def get_sg_ids_with_vpc_id(self, vpc_id: str):
        """
        Get security group ids of specific vpc.

        :param vpc_id: VpcId
        :return: GroupId
        """
        security_groups = self.cli.describe_security_groups()['SecurityGroups']
        result = [sg['GroupId'] for sg in security_groups if sg['VpcId'] == vpc_id]

        return result

    def get_eni_with_sg_id(self, security_group_id: str):
        """
        Get network interfaces with specific security group.

        :param security_group_id: GroupId
        :return: NetworkInterfaces
        """
        enis = self.cli.describe_network_interfaces()['NetworkInterfaces']
        result = [eni for eni in enis
                  if eni['Groups'] != []
                  and security_group_id in [x['GroupId'] for x in eni['Groups']]]

        return result

    def list_security_group_relations(self):
        """
        Describe security groups with related security groups, including egrss and ingress status.

        :return: SecurityGroupId, IsEgress, RelatedSecurityGroupId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        sg_ids = [sg_rule['GroupId'] for sg_rule in sg_rules]
        is_obs = [sg_rule['IsEgress'] for sg_rule in sg_rules]
        referenced_groups = [x['ReferencedGroupInfo']['GroupId'] if 'ReferencedGroupInfo' in x.keys() else None
                             for x in sg_rules]

        result = []
        for i in range(len(sg_rules)):
            result.append({'security_group_id': sg_ids[i],
                           'is_ob': is_obs[i],
                           'related_security_group_id': referenced_groups[i]})

        return result

    def get_related_security_groups(self, security_group_id: str):
        """
        Get the set of related security groups for specific security group.

        :param security_group_id: GroupId
        :return: GroupId
        """
        sg_relations = self.list_security_group_relations()

        result = [sg_relation['security_group_id'] for sg_relation in sg_relations
                  if sg_relation['related_security_group_id'] == security_group_id]
        return set(result)

    def get_security_group_rules(self, security_group_id: str):
        """
        Get security group rules of specific security group.

        :param security_group_id: GroupId
        :return: SecurityGroupRuleId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        result = [{'security_group_rule_id': sg_rule['SecurityGroupRuleId'],
                   'is_ob': True if sg_rule['IsEgress'] else False}
                  for sg_rule in sg_rules if sg_rule['GroupId'] == security_group_id]
        return result

    def revoke_security_group_rules(self, security_group_id: str):
        """
        Get rid of security group rule of specific security group.

        :param security_group_id: GroupId
        """
        sg_rules = self.get_security_group_rules(security_group_id)

        try:
            for sg_rule in sg_rules:
                if sg_rule['is_ob']:
                    self.cli.revoke_security_group_egress(GroupId=security_group_id,
                                                          SecurityGroupRuleIds=[sg_rule['security_group_rule_id']])
                else:
                    self.cli.revoke_security_group_ingress(GroupId=security_group_id,
                                                           SecurityGroupRuleIds=[sg_rule['security_group_rule_id']])
                self.logger.info('info')
        except:
            traceback.print_exc()

    def delete_security_groups(self, security_group_ids: list):
        """
        Delete useless and deletable security groups.

        :param security_group_ids: List of GroupId.
        :return:
        """
        try:
            for sg_id in security_group_ids:
                self.revoke_security_group_rules(sg_id)
                self.delete_security_group(sg_id)
                self.logger.info('info')
        except:
            traceback.print_exc()

    def delete_security_group(self, security_group_id: str):
        """
        Delete security group.

        :param security_group_id: GroupId
        """
        try:
            self.cli.delete_security_group(GroupId=security_group_id)
            self.logger.info('info')
        except:
            traceback.print_exc()

    def list_vpcs(self):
        """
        List one or more of your VPCs.

        :return: Vpcs
        """
        return self.cli.describe_vpcs()['Vpcs']

    def list_subnets(self):
        """
        List one or more of your Subnets.

        :return: Subnets
        """
        return self.cli.describe_subnets()['Subnets']

    def list_enis(self):
        """
        List one or more of your Network Interfaces.

        :return:
        """
        return self.cli.describe_network_interfaces()['NetworkInterfaces']

    def get_sg_id(self, group_name: str):
        """
        Retrieve subnet id from group name.

        :param group_name: GroupName
        :return: GroupId
        """
        return next(
            (i['GroupId'] for i in self.cli.describe_security_groups()['SecurityGroups']
             if group_name.lower() in i['GroupName'].lower()), None)

    def get_vpc_id(self, vpc_name: str):
        """
        Retrieve vpc id from vpc name

        :param vpc_name: vpc name
        :return:
        """
        return next(i['VpcId'] for i in self.list_vpcs() if 'Tags' in i
                    for t in i['Tags'] if vpc_name.lower() in t['Value'].lower())

    def get_subnet_id(self, vpc_id: str, subnet_name: str):
        """
        Retrieve subnet id from vpc id and subnet name.

        :param vpc_id: vpc_id
        :param subnet_name: subnet_name
        :return:
        """
        return next(s['SubnetId'] for s in self.list_subnets() if vpc_id == s['VpcId'] and 'Tags' in s
                    for t in s['Tags'] if subnet_name == t['Value'])

    def get_ec2_id(self, name):
        """

        :param name: ec2 instance name
        :return:
        """
        ec2 = boto3.resource('ec2')
        return next(
            i.id for i in ec2.instances.all() if i.state['Name'] == 'running' for t in i.tags if name == t['Value'])

    def describe_instances(self, instance_id_list: list = None):
        """

        Retrieve ec2 instance description.
        :param instance_id_list: ec2 instance id list
        :return:
        """
        if instance_id_list is not None:
            return self.cli.describe_instances(InstanceIds=instance_id_list)
        else:
            return self.cli.describe_instances()

    def get_ec2_instances_with_imds_v1(self):
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
