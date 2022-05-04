import os

import toml


class PoetryManager(object):

    def __init__(self):
        self.path = os.getcwd().replace("tests", "") if 'tests' in os.getcwd() else os.getcwd()

    def get_version(self):
        os.system("poetry update && poetry install && poetry build")
        return self.load_toml()['tool']['poetry']['version']

    def load_toml(self, filename='pyproject.toml'):
        return toml.load(os.path.join(self.path, filename))

    def save_toml(self, tml, filename='pyproject.toml'):
        with open(os.path.join(self.path, filename), 'w') as f:
            f.write(toml.dumps(tml))
