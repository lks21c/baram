import boto3
import traceback

from baram.log_manager import LogManager


class EFSManager(object):
    def __init__(self):
        self.cli = boto3.client('efs')
        self.logger = LogManager.get_logger()

    def list_file_systems(self):
        """
        List one or more of your EFS.

        :return:
        """
        return self.cli.describe_file_systems()['FileSystems']

    def list_mount_targets(self, file_system_id: str):
        """
        List mount targets of specific file system.

        :param file_system_id: FileSystemId
        :return:
        """
        try:
            return self.cli.describe_mount_targets(FileSystemId=file_system_id)['MountTargets']
        except:
            traceback.print_exc()

    def delete_file_systems(self, redundant_fs_ids: list = []):
        """
        Delete file systems of disused domains.

        :param redundant_fs_ids: list of FileSystemId.
        :return:
        """
        for fs_id in redundant_fs_ids:
            mount_target_ids = [mt['MountTargetId'] for mt in self.list_mount_targets(fs_id)]

            for mt_id in mount_target_ids:
                self.delete_mount_targets(mt_id)

            is_mount_targets_deleted = (len(self.list_mount_targets(fs_id)) == 0)
            while not is_mount_targets_deleted:
                is_mount_targets_deleted = (len(self.list_mount_targets(fs_id)) == 0)

            if is_mount_targets_deleted:
                self.delete_file_system(fs_id)

            self.logger.info('info')

    def list_redundant_file_systems(self, redundant_domain_ids: list = []):
        """
        Describe redundant file systems

        :param redundant_domain_ids: DomainId
        :return: List of FileSystemId
        """
        # TODO: Need more cases (sagemaker case only)
        redundant_fs_ids = [fs['FileSystemId'] for fs in self.list_file_systems()
                            if fs['CreationToken'] not in redundant_domain_ids
                            and 'sagemaker' in fs['Tags'][0]['Value']]
        return redundant_fs_ids

    def delete_mount_targets(self, mount_target_id: str):
        """
        Delete mount targets via its id.

        :param mount_target_id: MountTargetId
        :return:
        """
        try:
            self.cli.delete_mount_target(MountTargetId=mount_target_id)
        except:
            traceback.print_exc()

    def delete_file_system(self, file_system_id: str):
        """
        Delete specific file system.

        :param file_system_id: FileSystemId
        :return:
        """
        try:
            self.cli.delete_file_system(FileSystemId=file_system_id)
        except:
            traceback.print_exc()