import pdb
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
#
# class dbCalls:
#     def __init__(self, data_table, headers, regiondir, upload_table, proc_type, best_config, settings):
#         self.data_table = data_table
#         self.headers = headers
#         self.regiondir = regiondir
#         self.upload_table = upload_table
#         self.proc_type = proc_type
#         self.best_config = best_config
#         self.settings = settings
#
#     def createConnection():
#         '''
#         :return connection : the database connection object
#         :return cur : the cursor (temporary storage for retrieved data
#         '''
#         print('Connecting to database...')
#         conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
#         cur = conn.cursor()
#         return conn, cur






def createConnection():
    '''
    :return connection : the database connection object
    :return cur : the cursor (temporary storage for retrieved data
    '''
    print('Connecting to database...')
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn, cur


def removeTableDuplicates(table_name, headers):
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
        """.format(headers, table_name, table_name, table_name)
    return query


def addDataToTable(regiondir, upload_table, directories, proc_type, best_config, settings):
    '''
    Adds the confirmed_matches data to table
    :param table_name: the database table to which the confirmed matches will be addded
    :param directories:  directory variables
    :param proc_type: Process type, initially name_only
    :param upload_file: the dataframe containing the data
    :return: None
    '''

    upload_file = pd.read_csv(
        directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
        usecols=settings.dbUpload_cols)

    # # Filter manual matches file to just confirmed Yes matches and non-blank org id's
    confirmed_matches = upload_file[pd.notnull(upload_file['CH_id'])]

    confirmed_matches.to_csv(directories['confirmed_matches_file'].format(regiondir, proc_type),
                             columns=settings.dbUpload_cols,
                             index=False)

    conn, cur = createConnection()

    with open(directories['confirmed_matches_file'].format(regiondir, proc_type), 'r') as f:
        # Get headers dynamically
        reader = csv.reader(f)

        headers = next(reader, None)
        headers = ", ".join(headers)
        next(f)  # Skip header row
        # Input the data into the dedupe table
        # copy_expert allows access to csv methods (i.e. char escaping)
        cur.copy_expert(
            """COPY {}({}) from stdin (format csv)""".format(upload_table, headers), f)
        print("Data uploaded succesfully...")
    pdb.set_trace()
    query = removeTableDuplicates(upload_table, headers)
    cur.execute(query)
    conn.commit()
