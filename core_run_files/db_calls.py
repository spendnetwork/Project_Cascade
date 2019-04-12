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


def checkDataExists(regiondir, directories, in_args, data_source):
    '''
    Checks whether a public/registry datafile exists already, and if not prompts the user to download from remote sources via .env
    :param regiondir: root directory
    :param directories: dictionary containing various file/directories
    :param in_args: arguments containing variables such as file names and specific options
    :param data_source: database table
    :return: None
    '''
    # pdb.set_trace()

    # If public data doesn't exist:
    if not os.path.exists(directories['raw_dir'].format(regiondir) + directories['raw_pub_data'].format(in_args.pub_raw_name)):
        # If specific upload_to_db arg hasn't been passed (i.e. we're running for the first time)
        if not in_args.upload_to_db:
            choice = input("Public data not found, load from database? (y/n): ")
            if choice.lower() == 'y':
                # Check env file exists
                env_fpath = os.path.join(regiondir,'.env')
                if not os.path.exists(env_fpath):
                    print("Database credentials not found. Please complete the .env file using the '.env template'")
                    sys.exit()

                # Load public data
                query = createPublicDataSQLQuery(data_source)
                df = fetch_data(query)
                df.to_csv(directories['raw_dir'].format(regiondir) + directories['raw_pub_data'].format(in_args.pub_raw_name), index=False)
            else:
                print("Public/Registry data required - please copy in data csv to Data_Inputs\
                /Raw_Data or load from database")
                sys.exit()


def createPublicDataSQLQuery(source):
    """create query for pulling data from db"""

    query = \
        """
        SELECT
        json_doc::json->>'name'as org_name,
        json_doc::json->'locations'->'items'-> 0 ->'address'->>'streetName' as street_address1,
        json_doc::json->'locations'->'items'-> 1 ->'address'->>'streetName' as street_address2,
        json_doc::json->'locations'->'items'-> 2 ->'address'->>'streetName' as street_address3,
        json_doc::json->'id' as org_id
        from {}
        LIMIT 5000
        """.format(source)
    return query


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


def add_data_to_table(regiondir, table_name, directories, proc_type, man_matched, in_args):
    '''
    Adds the confirmed_matches data to table
    :param table_name: the database table to which the confirmed matches will be addded
    :param directories:  directory variables
    :param proc_type: Process type, initially name_only
    :param man_matched: the dataframe containing the data
    :return: None
    '''

    # Filter manual matches file to just confirmed Yes matches and non-blank org id's
    confirmed_matches = man_matched[pd.notnull(man_matched['org_id'])]
    if in_args.recycle:
        confirmed_matches = confirmed_matches[(man_matched['Manual_Match_NA'] == 'Y')]
    else:
        confirmed_matches = confirmed_matches[(man_matched['Manual_Match_N'] == 'Y')]

    confirmed_matches.to_csv(directories['confirmed_matches_file'].format(regiondir, proc_type),
                             columns=['priv_name', 'priv_address', 'org_id', 'org_name', 'pub_address'],
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