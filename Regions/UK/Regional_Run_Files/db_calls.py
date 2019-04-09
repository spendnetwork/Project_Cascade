import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import sys
import csv
import pdb

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


def fetch_data(query):
    """ retrieve data from the db using query"""
    conn, _ = create_connection()
    print('Importing data...')
    df = pd.read_sql(query, con=conn)
    conn.close()
    return df


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


def add_data_to_table(regiondir, table_name, directories, proc_type, upload_file):
    '''
    Adds the confirmed_matches data to table
    :param table_name: the database table to which the confirmed matches will be addded
    :param directories:  directory variables
    :param proc_type: Process type, initially name_only
    :param upload_file: the dataframe containing the data
    :return: None
    '''

    # Filter manual matches file to just confirmed Yes matches and non-blank org id's
    confirmed_matches = upload_file[pd.notnull(upload_file['CH_id'])]

    confirmed_matches = confirmed_matches[(upload_file['Manual_Match_N'] == 'Y')]

    confirmed_matches.to_csv(directories['confirmed_matches_file'].format(regiondir, proc_type),
                             columns=['priv_name', 'about_or_contact_text','company_url','home_page_text','CH_id', 'CH_name', 'CH_address'],
                             index=False)

    conn, cur = create_connection()
    with open(directories['confirmed_matches_file'].format(regiondir, proc_type), 'r') as f:
        # Get headers dynamically
        reader = csv.reader(f)
        headers = next(reader, None)
        headers = ", ".join(headers)
        next(f)  # Skip header row
        # Input the data into the dedupe table
        # copy_expert allows access to csv methods (i.e. char escaping)
        cur.copy_expert(
            """COPY {}({}) from stdin (format csv)""".format(table_name, headers), f)
        print("Data uploaded succesfully...")

    query = remove_table_duplicates(table_name, headers)
    cur.execute(query)
    conn.commit()
