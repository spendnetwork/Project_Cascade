import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import sys

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


def check_data_exists(config_dirs, in_args):
    # If public data doesn't exist:
    if not os.path.exists(config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name)):
        choice = input("Public data not found, load from database? (y/n): ")
        if choice.lower() == 'y':
            # Load public data
            data_source = "spaziodati.sd_sample"
            query = pull_public_data(data_source)
            df = fetch_data(query)
            df.to_csv(config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name))
        else:
            print("Public/Registry data required - please copy in data csv to Data_Inputs\
            /Raw_Data or load from database")
            sys.exit()


def pull_public_data(source):
    """create query for pulling data from db"""

    query = \
        """
        SELECT
        json_doc::json->>'name'as priv_name,
        json_doc::json->'locations'->'items'-> 0 ->'address'->>'streetName' as street_address1,
        json_doc::json->'locations'->'items'-> 1 ->'address'->>'streetName' as street_address2,
        json_doc::json->'locations'->'items'-> 2 ->'address'->>'streetName' as street_address3
        from {}
        LIMIT 500
        """.format(source)
    return query


def create_connection():
    print('connecting to db...')
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn, cur


def fetch_data(query):
    """ retrieve data from the db using query"""
    conn, _ = create_connection()
    print('importing data...')
    df = pd.read_sql(query, con=conn)
    conn.close()
    return df


def remove_duplicates(query):
    print("Remove duplicates from table...")
    # DELETE
    # FROM
    # people
    # WHERE
    # people.id
    # NOT
    # IN
    # (SELECT id FROM (
    # SELECT DISTINCT ON (firstname, lastname) *
    # FROM
    # people));
    pass

def add_data_to_table(table_name, config_dirs, proc_type, man_matched):
    """adds the confirmed_matches data to table"""

    # Filter manual matches file to just confirmed Yes matches and non-blank org id's
    confirmed_matches = man_matched[pd.notnull(man_matched['Org_ID'])]
    confirmed_matches = confirmed_matches[(man_matched['Manual_Match'] == 'Y')]
    confirmed_matches.to_csv(config_dirs['confirmed_matches_file'].format(proc_type),
                             columns=['priv_name', 'priv_address', 'Org_ID', 'org_name', 'pub_address'],
                             index=False)

    conn, cur = create_connection()
    with open(config_dirs['confirmed_matches_file'].format(proc_type), 'r') as f:
        next(f)  # Skip header row
        # Input the data into the dedupe table
        # copy_expert allows access to csv methods (i.e. char escaping)
        cur.copy_expert("""COPY {} from stdin (format csv)""".format(table_name), f)
    conn.commit()
    print("Data uploaded successfully.")

