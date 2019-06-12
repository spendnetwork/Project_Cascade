from shutil import copyfile
import pytest
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
import directories
from core_run_files import setup
from runfile import get_input_args

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")

# establish fpath to test directory
testdir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def in_args(tmp_root):
    args, parser = get_input_args(tmp_root,[])
    parser.add_argument('--src_raw_name', default='Data_Inputs/Raw_Data/src_data_raw_test.csv', type=str)
    parser.add_argument('--reg_raw_name', default='Data_Inputs/Raw_Data/reg_data_raw_test.csv', type=str)
    parser.add_argument('--src_adj_name', default='Data_Inputs/Adj_Data/src_data_adj_test.csv', type=str)
    parser.add_argument('--reg_adj_name', default='Data_Inputs/Adj_Data/reg_data_adj_test.csv', type=str)
    parser.add_argument('--assigned_file', default='Outputs/Name_Only/Deduped_Data/Name_Only_matched_clust_assigned.csv', type=str)
    parser.add_argument('--assigned_file', default='Outputs/Name_Only/Deduped_Data/Name_Only_matched_clust_assigned.csv', type=str)
    args = parser.parse_args([])
    return args


@pytest.fixture()
def test_src_df():
    df = pd.DataFrame()
    df['src_name'] = pd.Series(["Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A."])
    df['src_address'] = pd.Series(["3 Lala Street"])
    return df


@pytest.fixture()
def connection():
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn # CHECK THIS - SHOULD IT BE RETURN CUR???


@pytest.fixture(scope='session', autouse=True)
def tmp_root(tmpdir_factory):
    """tmpdir_factory fixture for the session scope containing the construction of the required working directories"""
    tmp_root =  tmpdir_factory.mktemp('tmproot')
    setup.setup_dirs(directories.dirs['dirs'], tmp_root)
    assert 1

    print("\n\nTemporary testing directories constructed at {}".format(str(tmp_root)))

    print("\n Copying over raw sample files/training files.")

    copyfile(str(testdir) + '/test_data/src_data_raw_test.csv', str(tmp_root) + '/Data_Inputs/Raw_Data/src_data_raw_test.csv')
    copyfile(str(testdir) + '/test_data/reg_data_raw_test.csv', str(tmp_root) + '/Data_Inputs/Raw_Data/reg_data_raw_test.csv')

    copyfile(str(testdir) + '/test_data/src_data_adj_test.csv',str(tmp_root) + '/Data_Inputs/Adj_Data/src_data_adj_test.csv')
    copyfile(str(testdir) + '/test_data/reg_data_adj_test.csv', str(tmp_root) + '/Data_Inputs/Adj_Data/reg_data_adj_test.csv')

    copyfile(str(testdir) + '/test_data/cluster_training.json', str(tmp_root) + '/Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json')
    copyfile(str(testdir) + '/test_data/matching_training.json', str(tmp_root) + '/Data_Inputs/Training_Files/Name_Only/Matching/matching_training.json')

    copyfile(str(testdir) + '/test_data/test_clustered_assigned.csv', str(tmp_root) + '/Outputs/Name_Only/Deduped_Data/Name_Only_matched_clust_assigned.csv')
    copyfile(str(testdir) + '/test_data/testconfig.py', str(tmp_root) + '/Config_Files/1_config.py')
    return tmp_root

