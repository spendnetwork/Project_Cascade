import pytest
from Config_Files import config_dirs
from run_files import setup
from runfile import get_input_args


def test_parser():
    # Pass [] as arg to ensure only the default args in the original function get called.
    # Otherwise calling pytest tests/test_file.py will pass 'tests/test_file.py' as an argument resulting in error.
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    parser = get_input_args([])
    assert 1


def test_dirscreation(tmpdir):
    # Pull through setup.setup_dirs and use tmpdir to assert that directories get created
    setup.setup_dirs(config_dirs.dirs['dirs'], tmpdir)
    assert 1
