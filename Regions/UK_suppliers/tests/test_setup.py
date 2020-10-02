import runfile
import pdb

def test_temp_root_exists(createTempProjectDirectory):
    assert createTempProjectDirectory

def test_settings_obj_exists(settings):
    assert settings

def test_raw_files_dirs_exist(transfer_data_files):
    pass

def test_clear_files():
    pass

def test_training_files():
    pass
    # test to check that if no training files present, the training flags get added and called individually.

