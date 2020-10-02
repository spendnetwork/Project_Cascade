import os
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from ..Regional_Run_Files.data_processing import DataProcessing, LevDist, AssignRegDataToClusters, ProcessSourceData, ProcessRegistryData
import pdb

#------------- UNIT TESTS ----------------

@pytest.fixture()
def test_src_df():

    df = pd.DataFrame()
    df['src_name'] = pd.Series(["Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A."])
    return df


@pytest.mark.parametrize("test_input, expected", [
    ("Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A.", "ditta abbott vascular knollravizza spa")])
def test_remvPunct(test_src_df, test_input, expected):

    input_index = test_src_df.src_name.str.find(test_input)
    test_src_df = DataProcessing.remvPunct('', test_src_df, 'src_name','src_name_adj')
    assert test_src_df.loc[input_index[0]].src_name_adj == expected


@pytest.mark.parametrize("test_input, expected", [
    ("ditta abbott vascular knollravizza ltd", "ditta abbott vascular knollravizza"),
    ("test-org plc", "test org"),
    ("test llp org", "test org")])
def test_shorten_name(settings, test_input, expected):
    # Names by this point have already been standardised (i.e. l.l.p = llp) therefore parametrized values are written as such
    assert LevDist.shortenName(settings.runfile_mods, test_input) == expected


@pytest.fixture()
def test_clustered_df(settings):
    filepath = os.path.join(settings.testdir, "{}")
    df_unassigned = pd.read_csv(filepath.format('/test_data/test_clustered.csv'))
    df_assigned = pd.read_csv(filepath.format('/test_data/test_clustered_assigned.csv'))
    return df_unassigned, df_assigned


def test_orgids_assigned_to_clusters(settings, test_clustered_df):
    # Tests that any cluster containing rows that have both a match and a none match gets
    # the match data copied over to the non match rows (highest Confidence_Score)

    df_processed = AssignRegDataToClusters.assign(settings, test_clustered_df[0])
    df_processed.to_csv('./df_processed.csv',index=False)
    assert_frame_equal(df_processed, test_clustered_df[1])

def test_addlevdist(settings, test_clustered_df):
    # Assert the levenshtein distance column can be added and populated

    levadded = LevDist(settings, test_clustered_df[1]).addLevDist()
    assert pd.notnull(levadded.at[0, 'leven_dist_N'])


def test_joinfields():
    # Not needed as only used when using Addresses too (i.e. joins string to addresses in one field)
    pass

# ------------- OVERALL TESTS ----------------

def test_srcdataprocessing(settings, createTempProjectDirectory, adjust_default_args):
    ''' tests src data cleaning correctly saves file to adj_data dir'''
    in_args = adjust_default_args
    tmp_root = createTempProjectDirectory
    ProcessSourceData(settings).clean()
    assert os.path.exists(os.path.join(tmp_root,settings.directories['adj_dir'].format(settings.region_dir),in_args.src_adj))

def test_regdataprocessing(settings, createTempProjectDirectory, adjust_default_args):
    ''' tests reg data cleaning correctly saves file to adj_data dir'''
    in_args = adjust_default_args
    tmp_root = createTempProjectDirectory
    ProcessRegistryData(settings).clean()
    assert os.path.exists(os.path.join(tmp_root,settings.directories['adj_dir'].format(settings.region_dir),in_args.reg_adj))


def test_adj():
    pass
    # test to make sure that adj col are lower case... noted from bug introduced

def test_short():
    pass
    # test to make sure that the short column doesnt contain any ltds etc...noted from bug introduced
