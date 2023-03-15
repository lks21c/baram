import boto3
import traceback

from baram.log_manager import LogManager


class EC2Manager(object):
    def __init__(self):
        self.cli = boto3.client('ec2')
        self.logger = LogManager.get_logger()

    def list_sgs(self):
        """
        Describes the specified security groups or all of your security groups.

        :return: SecurityGroups
        """
        try:
            return self.cli.describe_security_groups()['SecurityGroups']
        except:
            print(traceback.format_exc())
            return None

    def list_unused_sg_ids(self, description_filter: str = '', sm_domain_ids: list = []):
        """
        Describe useless and deletable security group ids.
        (including disused SageMaker domain related things)

        :param description_filter: String in 'Description'
        :param sm_domain_ids: DomainId of sagemaker
        :return: GroupIds
        """
        # 1. Get security groups not related to anything in ec2
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

    def list_vpc_sg_eni_subnets(self):
        """
        Return the list of all pairs of {VpcId, GroupId, NetworkInterfaceId, SubnetId}.

        :return: The list of {vpc_id, security_group_id, eni_id, subnet_id}
        """
        result = []
        vpcs = [vpc for vpc in self.list_vpcs() if vpc['State'] == 'available']
        vpc_ids = [vpc['VpcId'] for vpc in vpcs]
        specified_sg_ids = [self.get_sg_ids_with_vpc_id(vpc_id=vpc_id) for vpc_id in vpc_ids][0]
        specified_enis = [self.get_eni_with_sg_id(sg_id=sg_id) for sg_id in specified_sg_ids][0]

        try:
            for vpc_id in vpc_ids:
                for sg_id in specified_sg_ids:
                    pairs = {'vpc_id': vpc_id, 'sg_id': sg_id}
                    for eni in specified_enis:
                        pairs['eni_id'], pairs['subnet_id'] = eni['NetworkInterfaceId'], eni['SubnetId']
                        result.append(pairs)
            return result

        except TypeError:
            print(traceback.format_exc())
            return None

    def get_sg_ids_with_vpc_id(self, vpc_id: str):
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

    def get_eni_with_sg_id(self, sg_id: str):
        """
        Get network interfaces with specific security group.

        :param sg_id: GroupId
        :return: NetworkInterfaces
        """
        enis = self.cli.describe_network_interfaces()['NetworkInterfaces']
        return [eni for eni in enis if eni['Groups'] != [] and sg_id in [x['GroupId'] for x in eni['Groups']]]
        # try:
        #     return [eni for eni in enis if eni['Groups'] != [] and sg_id in [x['GroupId'] for x in eni['Groups']]]
        # except TypeError:
        #     print(traceback.format_exc())
        #     return None

    def list_sg_relations(self):
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
                   'related_sg_id':referenced_sgs[i]} for i in range(len(sg_rules))]
        return result

    def get_related_sgs(self, sg_id: str):
        """
        Get the set of related security groups for specific security group.

        :param sg_id: GroupId
        :return: GroupId
        """
        sg_relations = self.list_sg_relations()
        return set([sg_relation['sg_id'] for sg_relation in sg_relations if sg_relation['related_sg_id'] == sg_id])

    def get_sg_rules(self, sg_id: str):
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
            traceback.format_exc()

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
            traceback.format_exc()

    def delete_sg(self, sg_id: str):
        """
        Delete security group.

        :param sg_id: GroupId
        """
        try:
            self.cli.delete_sg(GroupId=sg_id)
            self.logger.info('security group has deleted')
        except:
            traceback.format_exc()

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

    def get_sg_id_with_sg_name(self, group_name: str):
        """
        Retrieve subnet id from group name.

        :param group_name: GroupName
        :return: GroupId
        """
        return next(
            (i['GroupId'] for i in self.cli.describe_security_groups()['SecurityGroups']
             if group_name.lower() in i['GroupName'].lower()), None)

    def get_vpc_id_with_vpc_name(self, vpc_name: str):
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

    def get_ec2_id_with_ec2_name(self, ec2_name: str):
        """

        :param ec2_name: ec2 instance name
        :return:
        """
        ec2 = boto3.resource('ec2')
        return next(
            i.id for i in ec2.instances.all() if i.state['Name'] == 'running' for t in i.tags if ec2_name == t['Value'])

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
