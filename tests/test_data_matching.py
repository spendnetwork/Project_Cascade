import ast
import pandas as pd
import pytest
import directories
from core_run_files import data_matching
import os


@pytest.fixture()
def load_samplerawdata(in_args, tmp_root):
    privrawPath = os.path.join(tmp_root, in_args.priv_raw_name)
    privraw = pd.read_csv(privrawPath)

    pubrawPath = os.path.join(tmp_root, in_args.pub_raw_name)
    pubraw = pd.read_csv(pubrawPath)
    return privrawPath, privraw, pubrawPath, pubraw
                            

@pytest.fixture()
def load_sampleadjdata(in_args, tmp_root):
    privadjPath = os.path.join(tmp_root, in_args.priv_adj_name)
    privadj = pd.read_csv(privadjPath)

    pubadjPath = os.path.join(tmp_root,in_args.pub_adj_name)
    pubadj = pd.read_csv(pubadjPath)
    return privadjPath, privadj, pubadjPath, pubadj

@pytest.fixture()
def load_clusteredData(in_args, tmp_root):
    clustDfPath= os.path.join(tmp_root, in_args.assigned_file)
    clustDf = pd.read_csv(clustDfPath)
    return clustDf


@pytest.fixture()
def load_testConfig():

    with open (os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data/testconfig.py')) as f:
        file_contents = []
        file_contents.append(f.read())
        config_file = ast.literal_eval(file_contents[0])
    return config_file


def test_extractMatches(load_clusteredData, tmp_root, load_testConfig):
    data_matching.extract_matches(tmp_root, load_clusteredData, load_testConfig, directories.dirs['dirs'], 1, 'Name_Only', 1)
    assert 1

def test_manualMatching():
    pass

#
#https://www.google.com/search?client=safari&rls=en&q=pytest+how+to+test+a+function+requiring+multiple+repetitive+user+input&ie=UTF-8&oe=UTF-8
#
# def test_dedupematchcluster(in_args, load_sampleadjdata, tmp_root):
#     pass
#     # with open(testdir + '/test_data/testconfig.py') as testconfig:
#     #     file_contents = []
#     #     file_contents.append(testconfig.read())
#     #
#     #     # Convert list to dictionary
#     #     testconfig = ast.literal_eval(file_contents[0])
#     #
#     # data_matching.dedupe_matchTEST(load_sampleadjdata[0], load_sampleadjdata[2], tmp_root, config_dirs.dirs['dirs'],
#     #                                testconfig, 'Name_Only', 1, in_args)
#     # assert 1w

#
# def test_matchprep(in_args, load_sampleadjdata, tmp_root):
#     data_matching.match_prep(tmp_root, config_dirs.dirs['dirs'],'Name_Only', in_args)
#     assert 1
#
#
#
