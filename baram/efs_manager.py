import boto3
import traceback

from baram.log_manager import LogManager


class EFSManager(object):
    def __init__(self):
        self.cli = boto3.client('efs')
        self.logger = LogManager.get_logger()

    def list_efs(self):
        """
        List one or more of your EFS.

        :return:
        """
        return self.cli.describe_file_systems()['FileSystems']

    def list_mount_targets(self, efs_id: str):
        """
        List mount targets of specific file system.

        :param efs_id: FileSystemId
        :return:
        """
        try:
            return self.cli.describe_mount_targets(FileSystemId=efs_id)['MountTargets']
        except:
            print(traceback.format_exc())
            return None

    def list_redundant_efs(self, redundant_sm_domain_ids: list = []):
        """
        Describe redundant file systems

        :param redundant_sm_domain_ids: DomainId
        :return: List of FileSystemId
        """
        # TODO: Need more cases (sagemaker case only)
        redundant_efs_ids = [efs['FileSystemId'] for efs in self.list_efs()
                             if efs['CreationToken'] not in redundant_sm_domain_ids
                             and 'sagemaker' in efs['Tags'][0]['Value']]
        return redundant_efs_ids

    def delete_mount_targets(self, mount_target_id: str):
        """
        Delete mount targets via its id.

        :param mount_target_id: MountTargetId
        :return:
        """
        try:
            self.cli.delete_mount_target(MountTargetId=mount_target_id)
        except:
            print(traceback.format_exc())

    def delete_efs(self, efs_id: str):
        """
        Delete specific file system.

        :param efs_id: FileSystemId
        :return:
        """
        try:
            mount_target_ids = [mt['MountTargetId'] for mt in self.list_mount_targets(efs_id)]

            for mt_id in mount_target_ids:
                self.delete_mount_targets(mt_id)

            is_mount_targets_remain = (len(self.list_mount_targets(efs_id)) != 0)
            while is_mount_targets_remain:
                is_mount_targets_remain = (len(self.list_mount_targets(efs_id)) != 0)

            if is_mount_targets_remain:
                self.cli.delete_efs(FileSystemId=efs_id)

            self.logger.info('efs has deleted')
        except:
            print(traceback.format_exc())
