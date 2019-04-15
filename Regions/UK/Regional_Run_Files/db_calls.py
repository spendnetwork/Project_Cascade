# Contains functions common to only UK matching

from core_run_files.db_calls import create_connection
import pandas as pd
import csv
import pdb


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
        """.format(headers, table_name, table_name, table_name)
    return query


def add_data_to_table(regiondir, table_name, directories, proc_type, best_config, dtypesmod):
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
        usecols=dtypesmod.dbUpload_cols)

    # # Filter manual matches file to just confirmed Yes matches and non-blank org id's
    confirmed_matches = upload_file[pd.notnull(upload_file['CH_id'])]

    confirmed_matches.to_csv(directories['confirmed_matches_file'].format(regiondir, proc_type),
                             columns=dtypesmod.dbUpload_cols,
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
    pdb.set_trace()
    query = remove_table_duplicates(table_name, headers)
    cur.execute(query)
    conn.commit()
