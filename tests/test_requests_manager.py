import pytest

from baram.requests_manager import RequestsManager


@pytest.fixture()
def rm():
    return RequestsManager()


def test_get(rm):
    # When
    response = rm.get('http://www.google.com')

    # Then
    assert response.text
    print(response.text)


def test_get_exception(rm):
    # When
    try:
        rm.get('leekwangsik')
    except:
        print('exception occurred as expected.')
