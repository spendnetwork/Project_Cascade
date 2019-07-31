import pdb
import csv
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
# from .data_matching import VerificationAndUploads
from runfile import Main, logging

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
            self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
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
            logging.info("Data uploaded succesfully...")

        query = self.removeTableDuplicates()
        cur.execute(query)
        conn.commit()


    def createConnection(self):
        '''
        :return connection : the database connection object
        :return cur : the cursor (temporary storage for retrieved data
        '''
        logging.info('Connecting to database...')
        conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
        cur = conn.cursor()
        return conn, cur

    def removeTableDuplicates(self):
        """
        :param table_name: the database table containing duplicates
        :param headers: the csv headers
        :return: the sql query to be executed
        """

        logging.info("Removing duplicates from table...")
        query = \
            """
            WITH dups AS 
                (SELECT DISTINCT ON ({}) * FROM {})

            DELETE FROM {} WHERE {}.id NOT IN
            (SELECT id FROM dups);
            """.format(self.headers, self.upload_table, self.upload_table, self.upload_table)
        return query

