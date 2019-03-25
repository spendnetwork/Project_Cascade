import pytest
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from runfile import get_input_args


# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")

@pytest.fixture
def in_args():
    in_args = get_input_args([])
    return in_args


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


# establish fpath to test directory
testdir = os.path.dirname(os.path.abspath(__file__))