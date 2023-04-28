import time
from subprocess import Popen, PIPE, STDOUT
from typing import Type


class ProcessManager(object):

    @classmethod
    def run_cmd(cls, cmd: str, sleep: bool = True) -> None:
        '''
        run shell command with standard output.

        :param cmd: shell command
        :param sleep: sleeps a second or not.
        :return:
        '''
        p: Popen = Popen(cmd, stdout=PIPE, stderr=STDOUT, shell=True)

        while True:
            line: str = p.stdout.readline()
            print(line.decode('utf-8'))
            if not line: break
            if sleep:
                time.sleep(1)
