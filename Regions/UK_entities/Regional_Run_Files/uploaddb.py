import pdb
import csv
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv
import os
import glob

# get the remote database details from .env
load_dotenv(find_dotenv())
host_remote = os.environ.get("HOST_REMOTE")
dbname_remote = os.environ.get("DBNAME_REMOTE")
user_remote = os.environ.get("USER_REMOTE")
password_remote = os.environ.get("PASSWORD_REMOTE")


def addDataToTable():
    '''
    Adds the confirmed_matches data to table
    :param table_name: the database table to which the confirmed matches will be addded
    :param directories:  directory variables
    :param proc_type: Process type, initially name_only
    :param upload_file: the dataframe containing the data
    :return: None
    '''

    conn, cur = createConnection()
    files = glob.glob(os.path.join('/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Outputs/Name_Only/Verified_Matches', '*'))
    for upload_file in files:
        with open(upload_file, 'r') as f:
            # Get headers dynamically
            reader = csv.reader(f)

            headers = next(reader, None)
            headers = ", ".join(headers)
            headers = headers
            next(f)  # Skip header row
            # Input the data into the dedupe table
            # copy_expert allows access to csv methods (i.e. char escaping)
            cur.copy_expert(
                """COPY {}({}) from stdin (format csv)""".format('matching.uk_entities', headers), f)

        conn.commit()

def createConnection():
    '''
    :return connection : the database connection object
    :return cur : the cursor (temporary storage for retrieved data
    '''
    print('Connecting to database...')
    conn = psy.connect(host=host_remote, dbname=dbname_remote, user=user_remote, password=password_remote)
    cur = conn.cursor()
    return conn, cur


if __name__ == '__main__':
    addDataToTable()