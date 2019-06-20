import sys
import pdb
import csv
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from runfile import Main

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")

class Db_Calls(Main):
    def __init__(self, settings):
        super().__init__(settings)

    def addDataToTable(self):
        '''
        Adds the confirmed_matches data to table
        :param table_name: the database table to which the confirmed matches will be addded
        :param directories:  directory variables
        :param proc_type: Process type, initially name_only
        :param upload_file: the dataframe containing the data
        :return: None
        '''

        upload_file = pd.read_csv(
            self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(
                self.best_config) + '.csv',
            usecols=self.dbUpload_cols)

        # # Filter manual matches file to just confirmed Yes matches and non-blank org id's
        confirmed_matches = upload_file[pd.notnull(upload_file['CH_id'])]

        confirmed_matches.to_csv(self.directories['confirmed_matches_file'].format(self.region_dir, self.proc_type),
                                 columns=self.dbUpload_cols,
                                 index=False)

        conn, cur = self.createConnection()

        with open(self.directories['confirmed_matches_file'].format(self.region_dir, self.proc_type), 'r') as f:
            # Get headers dynamically
            reader = csv.reader(f)

            headers = next(reader, None)
            headers = ", ".join(headers)
            self.headers = headers
            next(f)  # Skip header row
            # Input the data into the dedupe table
            # copy_expert allows access to csv methods (i.e. char escaping)
            cur.copy_expert(
                """COPY {}({}) from stdin (format csv)""".format(self.upload_table, self.headers), f)
            print("Data uploaded succesfully...")

        query = self.removeTableDuplicates()
        cur.execute(query)
        conn.commit()

    def createConnection(self):
        '''
        :return connection : the database connection object
        :return cur : the cursor (temporary storage for retrieved data
        '''
        print('Connecting to database...')
        conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
        cur = conn.cursor()
        return conn, cur

    def removeTableDuplicates(self, table_name, headers):
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


def checkDataExists(region_dir, directories, in_args, data_source):

    '''
    Checks whether a registry/registry datafile exists already, and if not prompts the user to download from remote sources via .env
    :param region_dir: root directory
    :param directories: dictionary containing various file/directories
    :param in_args: arguments containing variables such as file names and specific options
    :param data_source: database table
    :return: None
    '''

    # If registry data doesn't exist:
    if not os.path.exists(
            directories['raw_dir'].format(region_dir) + directories['raw_reg_data'].format(in_args.reg_raw_name)):
        # If specific upload arg hasn't been passed (i.e. we're running for the first time)
        if not in_args.upload:
            choice = input("Registry data not found, load from database? (y/n): ")
            if choice.lower() == 'y':
                # Check env file exists
                env_fpath = os.path.join('.', '.env')
                if not os.path.exists(env_fpath):
                    print("Database credentials not found. Please complete the .env file using the '.env template'")
                    sys.exit()

                # Load registry data
                query = createRegistryDataSQLQuery(data_source)
                df = fetchData(query)
                df.to_csv(
                    directories['raw_dir'].format(region_dir) + directories['raw_reg_data'].format(in_args.reg_raw_name),
                    index=False)
            else:
                print("Registry/Registry data required - please copy in data csv to Data_Inputs\
                /Raw_Data or load from database")
                sys.exit()


def createRegistryDataSQLQuery(source):
    """create query for pulling data from db"""

    query = \
        """
        SELECT
        json_doc::json->>'name'as reg_name,
        json_doc::json->'locations'->'items'-> 0 ->'address'->>'streetName' as street_address1,
        json_doc::json->'locations'->'items'-> 1 ->'address'->>'streetName' as street_address2,
        json_doc::json->'locations'->'items'-> 2 ->'address'->>'streetName' as street_address3,
        json_doc::json->'id' as reg_id
        from {}
        LIMIT 5000
        """.format(source)
    return query



def fetchData(query):
    """ retrieve data from the db using query"""
    conn, _ = Db_Calls.createConnection()
    print('Importing data...')
    df = pd.read_sql(query, con=conn)
    conn.close()
    return df


# def addDataToTable(region_dir, table_name, directories, proc_type, man_matched, in_args, dtypesmod):
#     '''
#     Adds the confirmed_matches data to table
#     :param table_name: the database table to which the confirmed matches will be addded
#     :param directories:  directory variables
#     :param proc_type: Process type, initially name_only
#     :param man_matched: the dataframe containing the data
#     :return: None
#     '''
#
#     # Filter manual matches file to just confirmed Yes matches and non-blank org id's
#     confirmed_matches = man_matched[pd.notnull(man_matched['reg_id'])]
#     if in_args.recycle:
#         confirmed_matches = confirmed_matches[(man_matched['Manual_Match_NA'] == 'Y')]
#     else:
#         confirmed_matches = confirmed_matches[(man_matched['Manual_Match_N'] == 'Y')]
#
#     confirmed_matches.to_csv(directories['confirmed_matches_file'].format(region_dir, proc_type),
#                              columns=dtypesmod.dbUpload_cols,
#                              index=False)
#
#     conn, cur = createConnection()
#     with open(directories['confirmed_matches_file'].format(region_dir, proc_type), 'r') as f:
#         # Get headers dynamically
#         reader = csv.reader(f)
#         headers = next(reader, None)
#         headers = ", ".join(headers)
#         next(f)  # Skip header row
#         # Input the data into the dedupe table
#         # copy_expert allows access to csv methods (i.e. char escaping)
#         cur.copy_expert(
#             """COPY {}({}) from stdin (format csv)""".format(table_name, headers), f)
#         print("Data uploaded succesfully...")
#
#     query = removeTableDuplicates(table_name, headers)
#     cur.execute(query)
#     conn.commit()
