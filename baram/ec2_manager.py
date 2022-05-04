import boto3


class EC2Manager(object):
    def __init__(self):
        self.cli = boto3.client('ec2')

    def list_security_groups(self):
        '''
        Describes the specified security groups or all of your security groups.

        :return: SecurityGroups
        '''
        return self.cli.describe_security_groups()['SecurityGroups']

    def list_vpcs(self):
        '''
        List one or more of your VPCs.
        :return: Vpcs
        '''
        return self.cli.describe_vpcs()['Vpcs']

    def list_subnet(self):
        '''
        List one or more of your Subnets.

        :return: Subnets
        '''
        return self.cli.describe_subnets()['Subnets']

    def get_sg_id(self, group_name: str):
        '''
        Retrieve subnet id from group name.

        :param group_name: group name
        :return: subnet id
        '''
        return next(
            (i['GroupId'] for i in self.list_security_groups()
             if group_name.lower() in i['GroupName'].lower()), None)

    def get_vpc_id(self, vpc_name: str):
        '''
        Retrieve vpc id from vpc name.
        :param vpc_name: vpc name
        :return:
        '''
        return next(i['VpcId'] for i in self.list_vpcs() if 'Tags' in i
                    for t in i['Tags'] if vpc_name.lower() in t['Value'].lower())

    def get_subnet_id(self, vpc_id: str, subnet_name: str):
        '''
        Retrieve subnet id from vpc id and subnet name.
        :param vpc_id: vpc_id
        :param subnet_name: subnet_name
        :return:
        '''
        return next(s['SubnetId'] for s in self.list_subnet() if vpc_id == s['VpcId'] and 'Tags' in s
                    for t in s['Tags'] if subnet_name == t['Value'])
