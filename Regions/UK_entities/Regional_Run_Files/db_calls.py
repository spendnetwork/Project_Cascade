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
import glob
import boto3
from botocore.exceptions import ClientError
# from core import database.


# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")


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

            DELETE FROM {} WHERE ({}.id) NOT IN
            (SELECT id FROM dups);
            """.format(self.headers, self.upload_table, self.upload_table, self.upload_table, self.upload_table)
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
        if not os.path.exists(self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(self.in_args.reg)):
            # Check env file exists
            env_fpath = os.path.join('.', '.env')
            if not os.path.exists(env_fpath):
                print("Database credentials not found. Please complete the .env file using the '.env template'")
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

            env_fpath = os.path.join('.', '.env')
            if not os.path.exists(env_fpath):
                print(
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
        """create query for pulling data from db"""
        print("Obtaining registry data...")
        query = \
            """
            SELECT
           legalname as reg_name,
           id as reg_id,
           '' as reg_address,
           scheme as reg_scheme,
           source as reg_source,
           created_at as reg_created_at
            from {}

            """.format(self.reg_data_source)
        return query

    def createSourceDataSQLQuery(self):
        """create query for pulling data from db"""
        print("Obtaining source data...")
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
               --AND t.json -> 'releases' -> 0 -> 'tag' ? 'tender'
               --AND t.json -> 'releases' -> 0 -> 'tag' ? 'award'
            ;
    
            """.format(self.src_data_source, "'" + self.in_args.data_date + "'")
        return query


    def fetchdata(self, query):
        """ retrieve data from the db using query"""
        conn, _ = self.db_calls.DbCalls.createConnection(self)
        print('Importing data...')
        df = pd.read_sql(query, con=conn)
        conn.close()
        return df


class AwsTransfers(Main):

    def __init__(self, settings):
        super().__init__(settings)
        self.bucket = 'org-matching'

    def transfer(self):
        '''
        Core function for the class. Checks args and either uploads to or downloads from s3 bucket
        :return: None
        '''
        # Upload unverified matches to s3 bucket
        if self.in_args.prodn_unverified:
            files = glob.glob(os.path.join(self.directories['unverified_matches_dir'].format(self.region_dir, self.proc_type), '*'))

            for filepath in files:
                filename = os.path.basename(filepath)
                self.upload_file(filepath, self.bucket, 'UK_entities/Unverified_Matches/' + filename)

        # Download verified matches from s3 bucket
        if self.in_args.prodn_verified:
            self.process_verified_files()

    def process_verified_files(self):
        # Scan s3 verified folder for files
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Verified_Matches/')

        # Ignore first file entry in dict as is just the folder name. Returns a list of files
        files = response['Contents'][1:]

        # For any files in /s3/verified/ - transfer them to local /verified_matches/
        for i in range(len(files)):
            s3.download_file(self.bucket,
                             files[i]['Key'],
                             os.path.join(self.directories['verified_matches_dir'].format(self.region_dir,
                                                                                 self.proc_type),
                                          os.path.basename(files[i]['Key'])))

        # Upload all files in verified_matches_dir (downloaded from /verified in s3 bucket:
        self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

        for i in range(len(files)):
            # Delete from s3 verified folder
            s3.delete_object(Bucket=self.bucket, Key=files[i]['Key'])

            try:
                # Delete from unverified folder (if hasn't been done by team already) so team know which haven't been
                # verified yet (located via date prefix of verified file incase of name change by team)
                response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Unverified_Matches/' + os.path.basename(files[i]['Key'])[:10])
                file = response['Contents'][:]
                s3.delete_object(Bucket=self.bucket, Key=file[i]['Key'])
            except:
                pass


    @staticmethod
    def upload_file(file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            print(e)
            return False
        print("Upload to S3 bucket complete!")
        return True


if __name__ == '__main__':
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

    # import boto3
    # >> > s3 = boto3.client('s3')
    # response = s3.upload_file(
    #     '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Outputs/Name_Only/holder/Matches_Buyers_DMadj.csv',
    #     'sn-orgmatching, ' Verified_matches / lalal.csv')