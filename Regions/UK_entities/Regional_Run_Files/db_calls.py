import sys
import pdb
import csv
import pandas as pd
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
from runfile import Main, logging
import glob


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

        conn, cur = self.createConnection()
        logging.info(f"Connected to {self.upload_table}")

        files = glob.glob(os.path.join(self.directories['verified_matches_dir'].format(self.region_dir, self.proc_type),'*'))
        for upload_file in files:
            with open(upload_file, 'r') as f:
                # Get headers dynamically
                reader = csv.reader(f)
                headers = next(reader, None)
                headers = ", ".join(headers)
                self.headers = headers
                next(f)  # Skip header row
                # Input the data into the dedupe table
                # copy_expert allows access to csv methods (i.e. char escaping)
                cur.copy_expert(
                    """COPY {}({}) from stdin (format csv)""".format(self.upload_table, headers), f)
                conn.commit()

        # Remove any exact duplicates from db table
        try:
            query = self.removeTableDuplicates()
            cur.execute(query)
            conn.commit()
        except:
            next

        # Also transfer matches to transfer table (orgs_lookup, where doesn't exist already)
        query = self.transferMatches()
        cur.execute(query)
        conn.commit()

    def createConnection(self):
        '''
        Creates a connection to the database specified .env

        :return connection : the database connection object
        :return cur : the cursor (temporary storage for retrieved data
        '''
        logging.info('Connecting to database...')
        conn = psy.connect(host=self.host_remote, dbname=self.dbname_remote, user=self.user_remote, password=self.password_remote)
        cur = conn.cursor()
        return conn, cur

    def removeTableDuplicates(self):
        """
        Remove any exact duplicates from db table

        :param table_name: the database table containing duplicates
        :param headers: the csv headers
        :return: the sql query to be executed
        """

        logging.info("Removing duplicates from table...")
        query = \
            """
            WITH dups AS 
                (SELECT DISTINCT ON ({}) * FROM {})

            DELETE FROM {} WHERE ({}.id) NOT IN
            (SELECT id FROM dups);
            """.format(self.headers, self.upload_table, self.upload_table, self.upload_table, self.upload_table)
        return query

    def transferMatches(self):
        logging.info(f"Tranferring new matches from {self.upload_table} to {self.transfer_table}.")
        query = \
        """
        INSERT INTO {}
            SELECT DISTINCT src_name, reg_scheme, reg_id, reg_name, match_source, match_date, match_by, created_at FROM {} m

            WHERE
                 NOT EXISTS (SELECT src_name, reg_name FROM {} t WHERE m.src_name = t.org_string)
                 AND m.manual_match_n LIKE 'Y'
        """.format(self.transfer_table, self.upload_table, self.transfer_table)
        return query

    def truncate_table(self, table):
        query = \
        """
        TRUNCATE TABLE {}
        """.format(table)
        return query

    def join_matches_to_orgs_lookup(self):

        query = \
        """
        SELECT
        count(t1.src_name)                                          script_string
      , count(oo.legalname)                                         database_matches
      , count(t1.reg_name)                                          script_matches
      , count(COALESCE(UPPER(oo.legalname), UPPER(t1.reg_name))) as merged_matches
        FROM
        matching.assigned_matches as t1
        LEFT JOIN ocds.orgs_lookup ol ON (UPPER(t1.src_name) = UPPER(ol.org_string))
        LEFT JOIN ocds.orgs_ocds oo ON (ol.scheme = oo.scheme AND ol.id = oo.id);
        """

        return query

    def upload_assigned_matches(self, conn, cur, assigned_file):

        with open(assigned_file, 'r') as f:
            # Get headers dynamically
            reader = csv.reader(f)
            headers = next(reader, None)
            headers = ", ".join(headers)

            self.headers = headers
            next(f)  # Skip header row
            # Input the data into the dedupe table
            # copy_expert allows access to csv methods (i.e. char escaping)
            cur.copy_expert(
                """COPY {}({}) from stdin (format csv)""".format('matching.assigned_matches', str(headers)), f)
            conn.commit()

class FetchData(DbCalls):

    '''
    Checks whether a registry/source datafile exists already, and if not prompts the user to download from remote sources
     via .env

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

        if not os.path.exists(self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(self.in_args.reg)):
            # Check env file exists
            env_fpath = self.dotenv_file
            if not os.path.exists(env_fpath):
                logging.info("Database credentials not found. Please complete the .env file using the '.env template'")
                sys.exit()

            # Load registry data
            query = self.db_calls.FetchData.createRegistryDataSQLQuery(self)
            df = self.db_calls.FetchData.fetchdata(self, query)
            df.to_csv(
                self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(self.in_args.reg),
                index=False)

        # If source data doesn't exist:
        if not os.path.exists(
                self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_src_data'].format(
                    self.in_args.src)):

            env_fpath = self.dotenv_file
            if not os.path.exists(env_fpath):
                logging.info(
                    "Database credentials not found. Please complete the .env file using the '.env template'")
                sys.exit()

            # Load source data

            query = self.db_calls.FetchData.createSourceDataSQLQuery(self)
            df = self.db_calls.FetchData.fetchdata(self, query)
            df.to_csv(
                self.directories['raw_dir'].format(self.region_dir) + self.directories[
                    'raw_src_data'].format(self.in_args.src),
                index=False)

    def createRegistryDataSQLQuery(self):
        """
        Create query for downloading registry data from db
        """
        logging.info("Obtaining registry data...")
        query = \
            """
            SELECT
           legalname as reg_name,
           id as reg_id,
           '' as reg_address,
           scheme as reg_scheme,
           'dedupe_script' as match_source,
           '' as created_at
            from {}

            """.format(self.reg_data_source)
        return query

    def createSourceDataSQLQuery(self):
        """
        Create query for pulling source data from db
        """

        logging.info("Obtaining source data...")
        if self.in_args.prodn:
            query = \
                """
                SELECT            
                distinct t.buyer as src_name,
                t.json -> 'releases' -> 0 -> 'tag' as src_tag,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'locality' as src_address_locality,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'postalCode' as src_address_postalcode,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'countryName' as src_address_countryname,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'streetAddress' as src_address_streetaddress,
                t.source as source
                
                  FROM {0} as t
                WHERE TRUE
                  AND (t.source in (
                      'cf_notices',
                      ''
                   )
                  OR (source = 'ted_notices' AND countryname = 'United Kingdom')
                  )
                  AND t.releasedate >= {1}
                  AND t.releasedate <= {2}
                   --AND t.json -> 'releases' -> 0 -> 'tag' ? 'tender'
                   --AND t.json -> 'releases' -> 0 -> 'tag' ? 'award'
                ;
        
                """.format(self.src_data_source, "'" + self.in_args.data_from_date + "'", "'" + self.in_args.data_to_date + "'")
        else:
            query = \
                """
                SELECT            
                distinct t.buyer as src_name,
                t.json -> 'releases' -> 0 -> 'tag' as src_tag,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'locality' as src_address_locality,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'postalCode' as src_address_postalcode,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'countryName' as src_address_countryname,
                t.json -> 'releases' -> 0 -> 'buyer' -> 'address' ->> 'streetAddress' as src_address_streetaddress,
                t.source as source

                  FROM {0} as t
                WHERE TRUE
                  AND t.releasedate >= '2009-09-11 00:00:00.000000'
                  AND t.releasedate <= '2015-09-11 00:00:00.000000'
                  AND countryname = 'United Kingdom'
                   
                   LIMIT 2000
                ;

                """.format(self.src_data_source)

        return query


    def fetchdata(self, query):
        """
        Retrieve data from the db using query
        """
        conn, _ = self.db_calls.DbCalls.createConnection(self)
        logging.info('Importing data...')
        df = pd.read_sql(query, con=conn)
        conn.close()
        return df
