import boto3


class EC2Manager(object):
    def __init__(self):
        self.cli = boto3.client('ec2')

    def list_security_groups(self):
        """
        Describes the specified security groups or all of your security groups.

        :return: SecurityGroups
        """
        sg = self.cli.describe_security_groups()['SecurityGroups']
        result = [x['GroupId'] for x in sg]
        return set(result)

    def list_instances(self):
        """

        :return:
        """
        instances = self.cli.describe_instances()['Reservations']
        result = [x['Instances'][0]['InstanceId'] for x in instances]
        return set(result)

    def list_security_group_id_with_instances(self, with_name: bool = False):
        """
        Describes all security groups tied to specific instance

        :param with_name:
        :return: SecurityGroups
        """
        instances = [x['Instances'][0] for x in self.cli.describe_instances()['Reservations']]
        vpc_with_instances = [x['VpcId'] for x in instances if x['State']['Name'] != 'terminated']
        security_groups = self.cli.describe_security_groups()['SecurityGroups']

        if with_name:
            result = [{x['GroupId']: x['GroupName']} for x in security_groups if x['VpcId'] in vpc_with_instances]
        else:
            result = [x['GroupId'] for x in security_groups if x['VpcId'] in vpc_with_instances]
        return set(result)

    def list_security_group_id_without_instances(self):
        """
        Describes all security groups tied to any instance

        :return: SecurityGroups
        """
        sg_total = self.list_security_groups()
        sg_with_instances = self.list_security_group_id_with_instances()
        return sg_total - sg_with_instances

    def list_redundant_security_group(self):
        """
        Describe useless and deletable security groups

        :return:
        """
        pass

    def list_security_group_relation(self):
        """
        Describe security groups with related security groups, including egrss and ingress status

        :return: SecurityGroupId, IsEgress, RelatedSecurityGroupId
        """
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        sg_id = [x['GroupId'] for x in sg_rules]
        is_ob = [x['IsEgress'] for x in sg_rules]
        referenced_groups = [x['ReferencedGroupInfo']['GroupId'] if 'ReferencedGroupInfo' in x.keys() else None
                             for x in sg_rules]

        result = []
        for i in range(len(sg_rules)):
            result.append({'security_group_id': sg_id[i],
                           'is_ob': is_ob[i],
                           'related_security_group_id': referenced_groups[i]})

        return result

    def get_related_security_groups(self, security_group_id: str):
        """
        Get the set of related security groups for specific security group

        :param security_group_id:
        :return: SecurityGroupId
        """
        sg_relation = self.list_security_group_relation()

        result = [x['security_group_id'] for x in sg_relation if x['related_security_group_id'] == security_group_id]
        return set(result)

    def get_security_group_rule(self, security_group_id: str):
        sg_rules = self.cli.describe_security_group_rules()['SecurityGroupRules']

        result = [{'security_group_rule_id': x['SecurityGroupRuleId'],
                   'security_group_id': x['ReferencedGroupInfo']['GroupId'],
                   'is_ob': True if x['IsEgress'] else False}
                  for x in sg_rules if x['GroupId'] == security_group_id]
        return result

    def revoke_security_group_rule(self, security_group_id: str):
        sg_rules = self.get_security_group_rule(security_group_id)

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

    def delete_security_group(self, security_group_id: str):
        self.cli.delete_security_group(GroupId=security_group_id)

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
            (i['GroupId'] for i in self.list_security_groups()
             if group_name.lower() in i['GroupName'].lower()), None)

    def get_vpc_id(self, vpc_name: str):
        """
        Retrieve vpc id from vpc name.
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

    def get_ec2_id(self, name):
        """

        :param name: ec2 instance name
        :return:
        """
        ec2 = boto3.resource('ec2')
        return next(i.id for i in ec2.instances.all() if i.state['Name'] == 'running' for t in i.tags if name == t['Value'])
