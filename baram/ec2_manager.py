import boto3
import botocore.exceptions

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
        security_groups = self.cli.describe_security_groups()['SecurityGroups']
        result = [x['GroupId'] for x in security_groups]
        return set(result)

    def list_instances(self):
        """


        :return:
        """
        instances = self.cli.describe_instances()['Reservations']
        result = [x['Instances'][0]['InstanceId'] for x in instances]
        return set(result)

    def delete_redundant_security_groups(self):
        """
        Delete useless and deletable security groups.

        :return:
        """
        redundant_security_groups = self.list_redundant_security_group_ids()

        if len(redundant_security_groups) > 0:
            self.revoke_security_group_rules(redundant_security_groups)
            self.delete_security_groups(redundant_security_groups)
        else:
            print("There's no redundant security groups to delete")

    def list_redundant_security_group_ids(self):
        """
        Describe useless and deletable security group ids.

        :return: SecurityGroupIds
        """
        vpc_sg_eni_subnets = self.list_vpc_sg_eni_subnets()
        security_group_ids_valid = set([pair['sg_id'] for pair in vpc_sg_eni_subnets])

        security_groups = self.cli.describe_security_groups()['SecurityGroups']
        security_group_ids_total = set([sg['GroupId'] for sg in security_groups])

        return security_group_ids_total - security_group_ids_valid

    def list_vpc_sg_eni_subnets(self):
        """
        return the list of all pairs of {vpc_id, security_group_id, eni_id, subnet_id}

        :param:
        :return: The list of {vpc_id, security_group_id, eni_id, subnet_id}
        """
        vpcs = [vpc for vpc in self.list_vpcs() if vpc['State'] == 'available']
        vpc_ids = [vpc['VpcId'] for vpc in vpcs]

        result = []
        for vpc_id in vpc_ids:
            specified_sg_ids = self.get_sg_ids_with_vpc_id(vpc_id=vpc_id)
            for sg_id in specified_sg_ids:
                specified_enis = self.get_eni_ids_with_sg_id(security_group_id=sg_id)
                pairs = {'vpc_id': vpc_id, 'sg_id': sg_id}
                if len(specified_enis) != 0:
                    for eni in specified_enis:
                        pairs['eni_id'] = eni['NetworkInterfaceId']
                        pairs['subnet_id'] = eni['SubnetId']
                result.append(pairs)

        return result

    def get_sg_ids_with_vpc_id(self, vpc_id: str):
        """

        :param vpc_id:
        :return: SecurityGroupId
        """
        security_groups = self.cli.describe_security_groups()['SecurityGroups']
        result = [sg['GroupId'] for sg in security_groups if sg['VpcId'] == vpc_id]

        return result

    def get_eni_ids_with_sg_id(self, security_group_id: str):
        """
        Get

        :param security_group_id:
        :return: NetworkInterfaces
        """
        enis = self.cli.describe_network_interfaces()['NetworkInterfaces']
        result = [eni for eni in enis
                  if eni['Groups'] != []
                  and security_group_id in [x['GroupId'] for x in eni['Groups']]]

        return result

    def list_security_group_relations(self):
        """
        Describe security groups with related security groups, including egrss and ingress status

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

        :param security_group_id:
        :return: SecurityGroupId
        """
        sg_relations = self.list_security_group_relations()

        result = [sg_relation['security_group_id'] for sg_relation in sg_relations
                  if sg_relation['related_security_group_id'] == security_group_id]
        return set(result)

    def get_security_group_rules(self, security_group_id: str):
        """
        Get security group rules of specific security group.

        :param security_group_id:
        :return: SecurityGroupRuleId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        result = [{'security_group_rule_id': sg_rule['SecurityGroupRuleId'],
                   'is_ob': True if sg_rule['IsEgress'] else False}
                  for sg_rule in sg_rules if sg_rule['GroupId'] == security_group_id]
        return result

    def revoke_security_group_rules(self, security_group_id_list: list):
        """
        Get rid of security group rule of security groups in list.

        :param security_group_id_list:
        """
        for sg_id in security_group_id_list:
            self.revoke_security_group_rule(sg_id)

    def revoke_security_group_rule(self, security_group_id: str):
        """
        Get rid of security group rule of specific security group.

        :param security_group_id:
        """
        sg_rules = self.get_security_group_rules(security_group_id)

        if len(sg_rules) > 0:
            for sg_rule in sg_rules:
                if sg_rule['is_ob']:
                    self.cli.revoke_security_group_egress(GroupId=security_group_id,
                                                          SecurityGroupRuleIds=[sg_rule['security_group_rule_id']])
                else:
                    self.cli.revoke_security_group_ingress(GroupId=security_group_id,
                                                           SecurityGroupRuleIds=[sg_rule['security_group_rule_id']])
                print(f"{security_group_id}'s {list(sg_rule.values())[0]} rule is deleted")
        else:
            print("There's no security group rule for this security group")

    def delete_security_groups(self, security_group_id_list: list):
        """
        Delete every security groups in input list.

        :param security_group_id_list:
        """
        for sg_id in security_group_id_list:
            self.delete_security_group(sg_id)

    def delete_security_group(self, security_group_id: str):
        """
        Delete specific security group.

        :param security_group_id:
        """
        try:
            self.cli.delete_security_group(GroupId=security_group_id)
            print(f"{security_group_id} is deleted")
        except botocore.exceptions.ClientError:
            self.logger.info('error')

    def list_vpcs(self):
        """
        List one or more of your VPCs.
        :return: Vpcs
        """
        return self.cli.describe_vpcs()['Vpcs']

    def list_subnet(self):
        """
        List one or more of your Subnets.

        :return: Subnets
        """
        return self.cli.describe_subnets()['Subnets']

    def get_sg_id(self, group_name: str):
        """
        Retrieve subnet id from group name.

        :param group_name: group name
        :return: subnet id
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
        return next(s['SubnetId'] for s in self.list_subnet() if vpc_id == s['VpcId'] and 'Tags' in s
                    for t in s['Tags'] if subnet_name == t['Value'])

    # def get_ec2_id(self, name):
    #     """
    #
    #     :param name: ec2 instance name
    #     :return:
    #     """
    #     ec2 = boto3.resource('ec2')
    #     return next(i.id for i in ec2.instances.all()
    #     if i.state['Name'] == 'running' for t in i.tags if name == t['Value'])
