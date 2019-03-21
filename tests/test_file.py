import pytest
import pandas as pd
import pdb
from run_files import data_processing, data_matching, setup
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from pandas.util.testing import assert_frame_equal
from Config_Files import config_dirs
import ast
# from runfile import get_input_args
import conftest


# get the remote database details from .env
from runfile import get_input_args

load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


# establish fpath to test directory
testdir = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture()
def test_priv_df():
    df = pd.DataFrame()
    df['priv_name'] = pd.Series(["Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A."])
    df['priv_address'] = pd.Series(["3 Lala Street"])
    return df


@pytest.fixture()
def connection():
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn


@pytest.mark.parametrize("test_input, expected", [
    ("Ditta ABBOTT VASCULAR Knoll-Ravizza S.p.A.", "ditta abbott vascular knollravizza spa")
])


def test_remvPunct(test_priv_df, test_input, expected):
    # Tests remvPunct ensure all punctuation removed and strings lowered
    pdb.set_trace()
    input_index = test_priv_df.priv_name.str.find(test_input)
    test_priv_df['priv_name_adj'] = data_processing.remvPunct(test_priv_df, 'priv_name','priv_name_adj')
    assert test_priv_df.loc[input_index[0]].priv_name_adj == expected


def isConnected(conn):
    if not conn or conn.closed == 1:
        return False
    return True


def test_connection(connection):
    assert isConnected(connection)


@pytest.mark.parametrize("test_input, expected", [
    ("ditta abbott vascular knollravizza spa", "ditta abbott vascular knollravizza"),
    ("test-org srl", "test org"),
    ("test srl org", "test org")])


def test_shorten_name(test_input, expected):
    assert data_processing.shorten_name(test_input) == expected


@pytest.fixture()
def test_clustered_df():

    filepath = os.path.join(testdir, "{}")
    df_unassigned = pd.read_csv(filepath.format('/test_clustered.csv'))
    df_assigned = pd.read_csv(filepath.format('/test_clustered_assigned.csv'))
    return df_unassigned, df_assigned


def test_orgids_assigned_to_clusters(test_clustered_df):
    # Tests that any cluster containing rows that have both a match and a none match gets
    # the match data copied over to the non match rows (highest confidence score)
    df_processed = data_processing.assign_pub_data_to_clusters(test_clustered_df[0])
    assert_frame_equal(df_processed, test_clustered_df[1])


@pytest.fixture()
def load_samplerawdata():
    privraw = pd.read_csv(testdir + '/priv_data_raw_test.csv')
    pubraw = pd.read_csv(testdir + '/pub_data_raw_test.csv')
    return privraw, pubraw


@pytest.fixture()
def load_sampleadjdata():
    privadj = pd.read_csv(testdir + '/priv_data_adj_test.csv')
    pubadj = pd.read_csv(testdir + '/pub_data_adj_test.csv')
    return privadj, pubadj


# def test_dedupematchcluster():
#     # pdb.set_trace()
#     with open(testdir + '/testconfig.py') as testconfig:
#         file_contents = []
#         file_contents.append(testconfig.read())
#
#         # Convert list to dictionary
#         configs = ast.literal_eval(file_contents[0])
#
#     data_matching.dedupe_match_cluster(testdir, config_dirs, testconfig, proc_type='Name_Only', proc_num=1)
#     assert 1

def test_parser():
    # Pass [] as arg to ensure only the default args in the original function get called.
    # Otherwise calling pytest tests/test_file.py will pass 'tests/test_file.py' as an argument resulting in error.
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    parser = get_input_args([])
    assert 1

def test_dedupematchcluster(load_sampleadjdata):
    pdb.set_trace()
    with open(testdir + '/testconfig.py') as testconfig:
        file_contents = []
        file_contents.append(testconfig.read())

        # Convert list to dictionary
        testconfig = ast.literal_eval(file_contents[0])
    data_matching.dedupe_matchTEST(load_sampleadjdata[0], load_sampleadjdata[1], testdir, config_dirs, testconfig, 'Name_Only', 1)
    assert 1


def test_addlevdist(test_clustered_df):
    # Assert the levenshtein distance column can be added and populated
    levadded = data_processing.add_lev_dist(test_clustered_df[1], None)
    assert pd.notnull(levadded.at[0, 'leven_dist'])


def test_dirscreation(tmpdir):
    # Pull through setup.setup_dirs and use tmpdir to assert that directories get created
    setup.setup_dirs(config_dirs.dirs['dirs'], tmpdir)
    assert 1


def test_parser():
    # Pass [] as arg to ensure only the default args in the original function get called.
    # Otherwise calling pytest tests/test_file.py will pass 'tests/test_file.py' as an argument resulting in error.
    # https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    parser = get_input_args([])
    assert parser

# Test line 87 onwards in runfile.py is functional. Maybe do a separate test for each mini function to ensure the whole thing can run.
# If one thing doesn't work here and the file gets
# still created then the rest of the if statement is skipped i.e. leven_dist etc.



# def test_raw_data_access():
#     # Test that either a raw data file exists or the connection can be made to the database
#     pass
#
# test imported data matches usecols format
#
# def test_if_private_file_exists_movetopublicdata():
#     # Test that if private adj file exists then skip to cleaning the public data file
#     # Could expand to checking that if each file exists then it moves on to the next section without
#     # duplicating or re-doing the process on the file that already exists.
#     pass
#
# def test_directory_creation():
#     #Tests if directories don't exist that they get created.

#
# def test_same_directories():
#     # Tests that

# Test that the data retrieved from the database consists of strings

# test upload data to db actually uploads it

# Use mocks to avoid having to make live-calls to a function that has an external function call inside it:
# https://realpython.com/python-cli-testing/#pytest
# def initial_transform(data):
#     outside_module.do_something()
#     return data



# test that delete duplicates within database actually returns a unique database
# pass in test_clustered.csv to the database, run deduplication on the table, then assert that it matches the test_deduplicated_data.csv
# could also combine this with a test_upload_to_db properly. Maybe upload test_clustered.csv and then assert that the table in the database is exactly equal to the original test_clustered.csv file.
# OR can test the sql query is an insert function as per the test_insert() function in the core repo (test_database.py)
# def test_uploadtodb():