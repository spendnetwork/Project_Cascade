import pytest
import pandas as pd
import pdb
from run_files import data_processing
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from pandas.util.testing import assert_frame_equal


# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


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

    curdir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(curdir, "{}")
    df_unassigned = pd.read_csv(filepath.format('/test_clustered.csv'))
    df_assigned = pd.read_csv(filepath.format('/test_clustered_assigned.csv'))
    return df_unassigned, df_assigned


def test_orgids_assigned_to_clusters(test_clustered_df):
    # Tests that any cluster containing rows that have both a match and a none match gets
    # the match data copied over to the non match rows (highest confidence score)
    df_processed = data_processing.assign_pub_data_to_clusters(test_clustered_df[0])
    assert_frame_equal(df_processed, test_clustered_df[1])


# def test_priv_data_dtypes():
#     # Test that the data types of the private data are correct
#     pass
#
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