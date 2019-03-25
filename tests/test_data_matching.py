import ast
import pandas as pd
import pytest
from run_files import data_matching
from tests.conftest import testdir


@pytest.fixture()
def load_samplerawdata():
    privraw = pd.read_csv(testdir + '/test_data/priv_data_raw_test.csv')
    pubraw = pd.read_csv(testdir + '/test_data/pub_data_raw_test.csv')
    return privraw, pubraw


@pytest.fixture()
def load_sampleadjdata():
    privadj = pd.read_csv(testdir + '/test_data/priv_data_adj_test.csv')
    pubadj = pd.read_csv(testdir + '/test_data/pub_data_adj_test.csv')
    return privadj, pubadj



def test_dedupematchcluster(in_args, load_sampleadjdata):
    with open(testdir + 'test_data/testconfig.py') as testconfig:
        file_contents = []
        file_contents.append(testconfig.read())

        # Convert list to dictionary
        testconfig = ast.literal_eval(file_contents[0])
    data_matching.dedupe_matchTEST(load_sampleadjdata[0], load_sampleadjdata[1], testdir, config_dirs, testconfig, 'Name_Only', 1, in_args.in_args)
    assert 1


# def test_dedupematchcluster():

#     with open(testdir + '/testconfig.py') as testconfig:
#         file_contents = []
#         file_contents.append(testconfig.read())
#
#         # Convert list to dictionary
#         configs = ast.literal_eval(file_contents[0])
#
#     data_matching.dedupe_match_cluster(testdir, config_dirs, testconfig, proc_type='Name_Only', proc_num=1)
#     assert 1