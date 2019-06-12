# Contains functions common to all regions
import csv
import pandas as pd

import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


def create_connection():
    '''
    :return connection : the database connection object
    :return cur : the cursor (temporary storage for retrieved data
    '''
    print('Connecting to database...')
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn, cur


def remove_table_duplicates(table_name, headers):
    """
    :param table_name: the database table containing duplicates
    :param headers: the csv headers
    :return: the sql query to be executed
    """

    print("Removing duplicates from table...")
    query = \
        """
        WITH dups AS 
            (SELECT DISTINCT ON ({}) * FROM {})
    
        DELETE FROM {} WHERE {}.id NOT IN
        (SELECT id FROM dups);
        """.format(headers, table_name, table_name,table_name)
    return query

