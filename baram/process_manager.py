import time
from subprocess import Popen, PIPE, STDOUT


class ProcessManager(object):

    @classmethod
    def run_cmd(cls, cmd, sleep=True):
        '''
        run shell command with standard output.

        :param cmd: shell command
        :param sleep: sleeps a second or not.
        :return:
        '''
        p = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)

        while True:
            line = p.stdout.readline()
            print(line.decode('utf-8'))
            if not line: break
            if sleep:
                time.sleep(1)
