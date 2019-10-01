import runfile
import pdb

def test_temp_root_exists(createTempProjectDirectory):
    assert createTempProjectDirectory

# def test_parser(tmp_root):
#
#     # Pass [] as arg to ensure only the default args in the original function get called.
#     # Otherwise calling pytest xyz.py will pass 'xyz.py' as an argument resulting in error.
#     # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
#     # args, parser = getInputArgs(tmp_root, ['--training'])
#     assert 1

def test_settings_obj_exists(create_settings_obj):
    assert create_settings_obj

