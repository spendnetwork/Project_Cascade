import sys
import pdb
import csv
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from pathlib import Path
import runfile
from runfile import Main
import settings

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


class DbCalls(Main):
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
            self.directories['manual_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(
                self.best_config) + '.csv',
            usecols=self.dbUpload_cols)

        # # Filter manual matches file to just confirmed Yes matches and non-blank org id's
        # confirmed_matches = upload_file[pd.notnull(upload_file['CH_id'])]
        #
        upload_file.to_csv(self.directories['confirmed_matches_file'].format(self.region_dir, self.proc_type),
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

    def removeTableDuplicates(self):
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
            """.format(self.headers, self.upload_table, self.upload_table, self.upload_table)
        return query

class FetchData(DbCalls):

    '''
    Checks whether a registry/source datafile exists already, and if not prompts the user to download from remote sources via .env
    :param region_dir: root directory
    :param directories: dictionary containing various file/directories
    :param in_args: arguments containing variables such as file names and specific options
    :param data_source: database table
    :return: None
    '''

    def __init__(self, settings):
        super().__init__(settings)

    def checkDataExists(self):
        # If registry data doesn't exist:
        if not os.path.exists(self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(self.in_args.reg_raw_name)):
            # If specific upload_to_db arg hasn't been passed (i.e. we're running for the first time)
            # if not self.in_args.upload_to_db:
            #     choice = input("Registry data not found, load from database? (y/n): ")
            #     if choice.lower() == 'y':
                    # Check env file exists
            env_fpath = os.path.join('.', '.env')
            if not os.path.exists(env_fpath):
                print("Database credentials not found. Please complete the .env file using the '.env template'")
                sys.exit()

            # Load registry data
            query = self.db_calls.FetchData.createRegistryDataSQLQuery(self)
            df = self.db_calls.FetchData.fetchData(self, query)
            df.to_csv(
                self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(self.in_args.reg_raw_name),
                index=False)

        # If source data doesn't exist:
        if not os.path.exists(
                self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_src_data'].format(
                    self.in_args.src_raw_name)):
            # If specific upload_to_db arg hasn't been passed (i.e. we're running for the first time)
            # if not self.in_args.upload_to_db:
            #     choice = input("Source data not found, load from database? (y/n): ")
            #     if choice.lower() == 'y':
            #         # Check env file exists

            env_fpath = os.path.join('.', '.env')
            if not os.path.exists(env_fpath):
                print(
                    "Database credentials not found. Please complete the .env file using the '.env template'")
                sys.exit()

            # Load source data
            query = self.db_calls.FetchData.createSourceDataSQLQuery(self)
            df = self.db_calls.FetchData.fetchData(self, query)
            df.to_csv(
                self.directories['raw_dir'].format(self.region_dir) + self.directories[
                    'raw_src_data'].format(self.in_args.src_raw_name),
                index=False)
                # else:
                #     print("Source data required - please copy in data csv to Data_Inputs\
                #        /Raw_Data or load from database")
                #     sys.exit()

    def createRegistryDataSQLQuery(self):
        """create query for pulling data from db"""

        query = \
            """
            SELECT
           legalname as reg_name,
           id as reg_id,
           '' as reg_address
            from {}
            
            """.format(self.reg_data_source)
        return query

    def createSourceDataSQLQuery(self):
        """create query for pulling data from db"""

        query = \
            """
            SELECT            
            distinct t.buyer as src_name,
            t.json -> 'releases' -> 0 -> 'tag' as src_tag,
            t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'locality' as src_address_locality,
            t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'postalCode' as src_address_postalcode,
            t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'countryName' as src_address_countryname,
            t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'streetAddress' as src_address_streetaddress
              FROM {0} as t
            WHERE TRUE
              AND (t.source in (
                  'cf_notices',
                  ''
               )
              OR (source = 'ted_notices' AND countryname = 'United Kingdom')
              )
              AND t.releasedate >= {1}
               --AND t.json -> 'releases' -> 0 -> 'tag' ? 'tender'
               --AND t.json -> 'releases' -> 0 -> 'tag' ? 'award'
            
            ;
    
            """.format(self.src_data_source, "'" + self.in_args.data_date + "'")
        return query


    def fetchData(self, query):
        """ retrieve data from the db using query"""
        conn, _ = self.db_calls.DbCalls.createConnection(self)
        print('Importing data...')
        df = pd.read_sql(query, con=conn)
        conn.close()
        return df

if __name__ == '__main__':
    #
    #
    rootdir = os.path.dirname(os.path.abspath(__file__))
    in_args, _ = runfile.getInputArgs(rootdir)

    if in_args.region == 'UK_entities':
        settings = settings.UK_entities

    settings.in_args = in_args
    settings.region_dir = os.path.join(rootdir, 'Regions', in_args.region)

    # Define config file variables and related data types file
    settings.config_path = Path(os.path.join(settings.region_dir, 'Config_Files'))

    # if not in_args
    DbCalls.addDataToTable(settings)