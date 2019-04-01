from runfile import get_input_args
import pdb


def test_parser(tmp_root):
    # Pass [] as arg to ensure only the default args in the original function get called.
    # Otherwise calling pytest xyz.py will pass 'xyz.py' as an argument resulting in error.
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    args, parser = get_input_args(tmp_root, ['--training'])
    assert 1
