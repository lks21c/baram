from baram.requests_manager import RequestsManager


def test_get():
    # When
    response = RequestsManager.get('http://www.google.com')

    # Then
    assert len(response.text) > 0
    print(response.text)


def test_get_exception():
    # When
    try:
        RequestsManager.get('leekwangsik')
    except:
        print('exception occurred as expected.')
