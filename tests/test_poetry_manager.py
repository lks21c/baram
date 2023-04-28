import os
import uuid
import pytest

from baram.poetry_manager import PoetryManager


@pytest.fixture()
def pm():
    return PoetryManager()


# def test_get_version(pm):
#     version = pm.get_version()
#     print(version)
#     assert version
#
#
# def test_save_load_toml(pm):
#     tml = pm.load_toml()
#
#     filename = str(uuid.uuid4())
#     pm.save_toml(tml, filename)
#
#     new_tml = pm.load_toml(filename)
#     print(new_tml)
#     assert new_tml
#
#     os.remove(os.path.join(pm.path, filename))
