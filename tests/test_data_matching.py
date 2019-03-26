import ast
import pandas as pd
import pytest
import pdb
from Config_Files import config_dirs
from run_files import data_matching
from tests.conftest import testdir
import subprocess


@pytest.fixture()
def load_samplerawdata(in_args, tmp_root):
    privrawPath = str(tmp_root + in_args.priv_raw_name)
    privraw = pd.read_csv(privrawPath)

    pubrawPath = str(tmp_root + in_args.pub_raw_name)
    pubraw = pd.read_csv(pubrawPath)
    return privrawPath, privraw, pubrawPath, pubraw
                            

@pytest.fixture()
def load_sampleadjdata(in_args, tmp_root):
    privadjPath = str(tmp_root + in_args.priv_adj_name)
    privadj = pd.read_csv(privadjPath)

    pubadjPath = str(tmp_root + in_args.pub_adj_name)
    pubadj = pd.read_csv(pubadjPath)
    return privadjPath, privadj, pubadjPath, pubadj


# def test_dedupematchcluster(in_args, load_sampleadjdata):
#
#     with open(testdir + '/test_data/testconfig.py') as testconfig:
#         file_contents = []
#         file_contents.append(testconfig.read())
#
#         # Convert list to dictionary
#         testconfig = ast.literal_eval(file_contents[0])
#
#     data_matching.dedupe_matchTEST(load_sampleadjdata[0], load_sampleadjdata[2], testdir, config_dirs.dirs['dirs'], testconfig, 'Name_Only', 1, in_args)
#     assert 1

def test_dedupematchcluster(in_args, load_sampleadjdata, tmp_root):
    with open(testdir + '/test_data/testconfig.py') as testconfig:
        file_contents = []
        file_contents.append(testconfig.read())

        # Convert list to dictionary
        testconfig = ast.literal_eval(file_contents[0])

    data_matching.dedupe_matchTEST(load_sampleadjdata[0], load_sampleadjdata[2], tmp_root, config_dirs.dirs['dirs'],
                                   testconfig, 'Name_Only', 1, in_args)
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