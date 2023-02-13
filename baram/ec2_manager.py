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

    def get_ec2_id(self, name):
        '''

        :param name: ec2 instance name
        :return:
        '''
        ec2 = boto3.resource('ec2')
        return next(
            i.id for i in ec2.instances.all() if i.state['Name'] == 'running' for t in i.tags if name == t['Value'])

    def describe_instance(self, instance_id_list: list = None):
        '''

        Retrieve ec2 instance description.
        :param instance_id: ec2 instance id
        :return:
        '''
        if instance_id_list is not None:
            return self.cli.describe_instances(InstanceIds=instance_id_list)
        else:
            return self.cli.describe_instances()

    def get_ec2_instances_with_imds_v1(self):
        '''

        :return: get ec2 instances that support imds_v1.
        '''
        response = self.describe_instance()
        return [i['InstanceId'] for r in response['Reservations']
                for i in r['Instances'] if i['MetadataOptions']['HttpTokens'] != 'required']

    def apply_imdsv2_only_mode(self, instances_list: list = None):
        '''

        Apply imdsv2 only mode into ec2 instances.
        :param instances_list:
        :return:
        '''
        for i in instances_list:
            self.cli.modify_instance_metadata_options(InstanceId=i,
                                                      HttpTokens='required',
                                                      HttpPutResponseHopLimit=1,
                                                      HttpEndpoint='enabled'
                                                      )
