from shutil import copyfile
import pytest
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
import directories
from Regions.UK_entities.Regional_Run_Files import setup
import runfile
import pdb
from pathlib import Path

'''
Need to create in_args first as this sets default settings region
Then create settings object passing in in_args
'''

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")

# establish filepath to current test directory
testdir = os.path.dirname(os.path.abspath(__file__))

@pytest.fixture(scope='session', autouse=True)
def createTempProjectDirectory(tmpdir_factory):
    ''' Create a temporary root directory from which the other project folders will be creating for testing '''
    temprootdir = tmpdir_factory.mktemp('tmproot')
    return temprootdir


@pytest.fixture(scope='session', autouse=True)
def adjust_default_args(createTempProjectDirectory):
    ''' Create arg_parser from runfile but also add additional arguments necessary to locate the test files '''
    args, parser = runfile.getInputArgs(createTempProjectDirectory,[])
    parser.add_argument('--src_raw_name', default='Data_Inputs/Raw_Data/src_data_raw_test.csv', type=str)
    parser.add_argument('--reg_raw_name', default='Data_Inputs/Raw_Data/reg_data_raw_test.csv', type=str)
    parser.add_argument('--src_adj_name', default='Data_Inputs/Adj_Data/src_data_adj_test.csv', type=str)
    parser.add_argument('--reg_adj_name', default='Data_Inputs/Adj_Data/reg_data_adj_test.csv', type=str)
    parser.add_argument('--assigned_file', default='Outputs/Name_Only/Deduped_Data/Name_Only_matched_clust_assigned.csv', type=str)
    # Empty list as param below to avoid i.e. 'test_setup.py' being passed as an argument
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    args = parser.parse_args([])
    return args


@pytest.fixture(scope='session')
def create_settings_obj(adjust_default_args, createTempProjectDirectory):

    import settings

    # settings = runfile.createSettingsObj(createTempProjectDirectory, adjust_default_args, settings)
    in_args = adjust_default_args

    if in_args.region == 'Italy':
        settings = settings.Italy_Settings

    if in_args.region == 'UK':
        settings = settings.UK_Settings

    if in_args.region == 'UK_entities':
        settings = settings.UK_entities

    if in_args.region == 'CQC':
        settings = settings.CQC_settings

    settings.in_args = in_args
    settings.region_dir = os.path.join(createTempProjectDirectory, 'Regions', in_args.region)

    # Define config file variables and attach to settings object
    settings.config_path = Path(os.path.join(settings.region_dir, 'Config_Files'))

    return settings


@pytest.fixture(scope='session', autouse=True)
def transfer_data_files(create_settings_obj, createTempProjectDirectory):

    settings = create_settings_obj
    tmp_root = createTempProjectDirectory

    setup.Setup(settings).setupRawDirs()
    setup.Setup(settings).SetupDirs()

    print("\n\nTemporary testing directories constructed at {}".format(str(tmp_root)))

    copyfile(str(testdir) + '/test_data/src_data_raw_test.csv',
             os.path.join(settings.directories['raw_dir'].format(settings.region_dir),'src_data_raw_test.csv'))

    copyfile(str(testdir) + '/test_data/reg_data_raw_test.csv',
             os.path.join(settings.directories['raw_dir'].format(settings.region_dir), 'reg_data_raw_test.csv'))

    copyfile(str(testdir) + '/test_data/src_data_adj_test.csv',
             os.path.join(settings.directories['adj_dir'].format(settings.region_dir), 'src_data_adj_test.csv'))

    copyfile(str(testdir) + '/test_data/reg_data_adj_test.csv',
             os.path.join(settings.directories['adj_dir'].format(settings.region_dir), 'reg_data_adj_test.csv'))

    copyfile(str(testdir) + '/test_data/cluster_training.json',
             os.path.join(settings.directories['proc_type_train_clust_dir'].format(settings.region_dir, settings.proc_type), 'clustering_training.json'))

    copyfile(str(testdir) + '/test_data/matching_training.json',
             os.path.join(settings.directories['proc_type_train_match_dir'].format(settings.region_dir, settings.proc_type),
                          'matching_training.json'))

    copyfile(str(testdir) + '/test_data/test_clustered_assigned.csv',
             settings.directories['assigned_output_file'].format(settings.region_dir, settings.proc_type))

    copyfile(str(testdir) + '/test_data/testconfig.py', os.path.join(settings.region_dir, 'Config_Files', '1_config.py'))

    return tmp_root

#
#
#
#
#
#
# # @pytest.fixture()
# def test_src_df(tmp_root):
#     pdb.set_trace()
#     df = pd.DataFrame()
#     df['src_name'] = pd.Series(["Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A."])
#     df['src_address'] = pd.Series(["3 Lala Street"])
#     return df
#     # assert df
#
#
# @pytest.fixture()
# def connection():
#     conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
#     cur = conn.cursor()
#     return conn # CHECK THIS - SHOULD IT BE RETURN CUR???
#
#
#
