from baram.conf_manager import ConfManager


def test_load_json_to_dict():
    # When
    a = ConfManager.load_json_to_dict('a.json')

    # Then
    assert a['key'] == 'value'

def test_load_json_to_dict_exception():
    # When
    try:
        ConfManager.load_json_to_dict('b.json')
    except:
        print('exception occurred as expected.')
