import os
import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal
from core_run_files import data_processing
from tests.conftest import testdir


@pytest.mark.parametrize("test_input, expected", [
    ("Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A.", "ditta abbott vascular knollravizza spa")
])

def test_remvPunct(test_src_df, test_input, expected):
    # Tests remvPunct ensure all punctuation removed and strings lowered
    input_index = test_src_df.src_name.str.find(test_input)
    test_src_df['src_name_adj'] = data_processing.remvPunct(test_src_df, 'src_name','src_name_adj')
    assert test_src_df.loc[input_index[0]].src_name_adj == expected


@pytest.mark.parametrize("test_input, expected", [
    ("ditta abbott vascular knollravizza spa", "ditta abbott vascular knollravizza"),
    ("test-org srl", "test org"),
    ("test srl org", "test org")])


def test_shorten_name(test_input, expected):
    assert data_processing.shorten_name(test_input) == expected


@pytest.fixture()
def test_clustered_df():

    filepath = os.path.join(testdir, "{}")
    df_unassigned = pd.read_csv(filepath.format('/test_data/test_clustered.csv'))
    df_assigned = pd.read_csv(filepath.format('/test_data/test_clustered_assigned.csv'))
    return df_unassigned, df_assigned


def test_orgids_assigned_to_clusters(test_clustered_df):
    # Tests that any cluster containing rows that have both a match and a none match gets
    # the match data copied over to the non match rows (highest confidence score)
    df_processed = data_processing.assign_reg_data_to_clusters(test_clustered_df[0])
    assert_frame_equal(df_processed, test_clustered_df[1])


def test_addlevdist(test_clustered_df):
    # Assert the levenshtein distance column can be added and populated
    levadded = data_processing.add_lev_dist(test_clustered_df[1], None)
    assert pd.notnull(levadded.at[0, 'leven_dist'])

