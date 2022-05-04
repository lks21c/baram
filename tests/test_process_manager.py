from baram.process_manager import ProcessManager


def test_get_version():
   ProcessManager.run_cmd('ls -al', False)