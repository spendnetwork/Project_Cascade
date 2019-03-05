import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")

def construct_query(source):
    """create query for pulling data from db"""

    query =\
    """
    SELECT
    json_doc::json->>'name'as priv_name,
    json_doc::json->'locations'->'items'-> 0 ->'address'->>'streetName' as street_address1,
    json_doc::json->'locations'->'items'-> 1 ->'address'->>'streetName' as street_address2,
    json_doc::json->'locations'->'items'-> 2 ->'address'->>'streetName' as street_address3
    from {}
    LIMIT 50
    """.format(source)
    return query

def fetch_data(query):
    """ retrieve data from the db using query"""
    print ('connecting to db...')
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    print ('importing data...')
    df = pd.read_sql(query, con=conn)
    conn.close()

    return df

def add_data_to_table(table_name):
    """adds the confirmed_matches data to table"""

    # Input the data into the dedupe table
    # make new connection
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor() # call cursor() to be able to write to the db
    with open(config_dirs['confirmed_matches_file'].format(proc_type), 'r') as f:
        next(f) # Skip header row
        # copy_expert allows access to csv methods (i.e. char escaping)
        cur.copy_expert("""COPY {} from stdin (format csv)""".format(table_name), f)
    conn.commit()

