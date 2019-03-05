import pdb
import argparse
import pandas as pd
import os
import subprocess
from pathlib import Path
from fuzzywuzzy import fuzz
from tqdm import tqdm
import numpy as np
import ast
import json
import org_suffixes
from Config_Files import config_dirs
import string
from shutil import copyfile
import psycopg2 as psy
from dotenv import load_dotenv, find_dotenv

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


def get_input_args():
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    parser = argparse.ArgumentParser()
    parser.add_argument('--priv_raw_name', default='private_data.csv', type=str,
                        help='Set raw private/source datafile name')
    parser.add_argument('--pub_raw_name', default='public_data.csv', type=str, help='Set raw public datafile name')
    parser.add_argument('--priv_adj_name', default='priv_data_adj.csv', type=str,
                        help='Set cleaned private/source datafile name')
    parser.add_argument('--pub_adj_name', default='pub_data_adj.csv', type=str, help='Set cleaned public datafile name')
    parser.add_argument('--recycle', action='store_true', help='Recycle the manual training data')
    parser.add_argument('--training', action='store_false', help='Modify/contribute to the training data')
    args = parser.parse_args()
    return args


def clean_private_data(config_dirs):
    """
	Takes the private data file as input, org type suffixes are replaced with abbreviated versions
	and strings reformatted for consistency across the two datasets

	:return df: the amended private datafile
	"""
    raw_data = config_dirs['raw_dir'] + config_dirs['raw_priv_data'].format(in_args.priv_raw_name)
    adj_data = config_dirs['adj_dir'] + config_dirs['adj_priv_data'].format(in_args.priv_adj_name)

    if not os.path.exists(adj_data):
        df = pd.read_csv(raw_data, usecols=['id', 'supplier_name', 'supplier_streetadd'],
                         dtype={'supplier_name': np.str, 'supplier_streetadd': np.str})
        df.rename(columns={'supplier_name': 'priv_name', 'supplier_streetadd': 'priv_address'}, inplace=True)
        print("Re-organising private data...")
        # Remove punctuation and double spacing
        df['priv_name_adj'] = df['priv_name'].str.translate(
            str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
        df['priv_name_adj'].replace(org_suffixes.org_type_dict, regex=True, inplace=True)
        print("...done")
        df.to_csv(adj_data, index=False)
    else:
        # Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
        df = pd.read_csv(adj_data, usecols=['id', 'priv_name', 'priv_name_adj', 'priv_address'],
                         dtype={'id': np.str, 'priv_name': np.str, 'priv_address': np.str, 'priv_name_adj': np.str})
    return df


def clean_public_data(config_dirs):
    """
	Takes the raw public data file and splits into chunks.
	Multiple address columns are merged into one column,
	org type suffixes are replaced with abbreviated versions and strings reformatted for consistency across the two datasets

	:return dffullmerge: the public dataframe adjusted as above
	"""
    raw_data = config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name)
    adj_data = config_dirs['adj_dir'] + config_dirs['adj_pub_data'].format(in_args.pub_adj_name)

    if not os.path.exists(adj_data):
        print("Re-organising public data...")
        df = pd.read_csv(raw_data,
                         usecols={'org_name', 'street_address1', 'street_address2', 'street_address3', 'Org_ID'},
                         dtype={'org_name': np.str, 'street_address1': np.str, 'street_address2': np.str,
                                'street_address3': np.str, 'Org_ID': np.str},
                         chunksize=500000)

        dffullmerge = pd.DataFrame([])
        for chunk in df:
            # Remove punctuation and double spacing
            chunk['pub_name_adj'] = chunk['org_name'].str.translate(
                str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
            chunk['pub_name_adj'].replace(org_suffixes.org_type_dict, regex=True, inplace=True)
            ls = []
            # Merge multiple address columns into one column
            for idx, row in tqdm(chunk.iterrows()):
                ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row['street_address1']]))
                for key in ['street_address2', 'street_address3']:
                    if pd.notnull(row[key]):
                        ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row[key]]))
            labels = ['Org_ID', 'org_name', 'pub_name_adj', 'street_address']
            dfmerge = pd.DataFrame.from_records(ls, columns=labels)
            dffullmerge = pd.concat([dffullmerge, dfmerge], ignore_index=True)
        dffullmerge.drop_duplicates(inplace=True)
        dffullmerge.rename(columns={'street_address': 'pub_address'}, inplace=True)
        print("...done")

        dffullmerge.to_csv(adj_data, index=False)
    else:
        dffullmerge = pd.read_csv(adj_data, usecols=['Org_ID', 'org_name', 'pub_name_adj', 'pub_address'],
                                  dtype={'Org_ID': np.str, 'org_name': np.str, 'pub_name_adj': np.str,
                                         'pub_address': np.str})
    return dffullmerge


def dedupe_match_cluster(dirs, configs, proc_type, proc_num):
    """
	Deduping - first the public and private data are matched using dedupes csvlink,
	then the matched file is put into clusters
	:param dirs: file/folder locations
	:param  configs: the main config files
	:param proc_type: the 'type' of the process (Name, Name & Address)
	:param proc_num: the individual process within the config file
	:return None
	:output : matched output file
	:output : matched and clustered output file
	"""

    priv_fields = configs['processes'][proc_type][proc_num]['dedupe_field_names']['private_data']
    pub_fields = configs['processes'][proc_type][proc_num]['dedupe_field_names']['public_data']

    priv_file = dirs['adj_dir'] + dirs['adj_priv_data']
    pub_file = dirs['adj_dir'] + dirs['adj_pub_data']
    # Matching:
    if not os.path.exists(dirs['match_output_file'].format(proc_type)):
        if in_args.recycle == True:
            # Copy manual matching file over to build on for clustering
            copyfile(config_dirs['manual_matching_train_backup'], config_dirs['manual_training_file'].format(proc_type))

        # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
        if os.path.exists('./learned_settings'):
            os.remove('./learned_settings')
        print("Starting matching...")
        cmd = ['csvlink '
               + str(priv_file).format(in_args.priv_adj_name) + ' '
               + str(pub_file).format(in_args.pub_adj_name)
               + ' --field_names_1 ' + ' '.join(priv_fields)
               + ' --field_names_2 ' + ' '.join(pub_fields)
               + ' --skip_training ' + str(in_args.training)
               + ' --training_file ' + dirs['manual_training_file'].format(proc_type)
               + ' --output_file ' + dirs['match_output_file'].format(proc_type)]
        p = subprocess.Popen(cmd, shell=True)
        p.wait()
        df = pd.read_csv(dirs['match_output_file'].format(proc_type),
                         usecols=['id', 'priv_name', 'priv_address', 'priv_name_adj', 'Org_ID', 'pub_name_adj',
                                  'pub_address'],
                         dtype={'id': np.str, 'priv_name': np.str, 'priv_address': np.str, 'priv_name_adj': np.str,
                                'Org_ID': np.str, 'pub_name_adj': np.str, 'pub_address': np.str})
        df = df[pd.notnull(df['priv_name'])]
        df.to_csv(dirs['match_output_file'].format(proc_type), index=False)

    # Clustering:
    if not os.path.exists(dirs['cluster_output_file'].format(proc_type)):
        # Copy training file from first clustering session if recycle mode
        if in_args.recycle == True:
            copyfile(config_dirs['cluster_training_backup'], config_dirs['cluster_training_file'].format(proc_type))

        print("Starting clustering...")
        cmd = ['python csvdedupe.py '
               + dirs['match_output_file'].format(proc_type) + ' '
               + ' --field_names ' + ' '.join(priv_fields)
               + ' --skip_training ' + str(in_args.training)
               + ' --training_file ' + dirs['cluster_training_file'].format(proc_type)
               + ' --output_file ' + dirs['cluster_output_file'].format(proc_type)]
        p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
        p.wait()  # wait for subprocess to finish

        if in_args.recycle == False:
            # Copy training file to backup, so it can be found and copied into recycle phase clustering
            copyfile(dirs['cluster_training_file'].format(proc_type), config_dirs['cluster_training_backup'])
    else:
        pass


def shorten_name(row):
    """
	Removes the company suffixes according to the org_suffixes.org_type_dict. This helps with the extraction phase
	because it improves the relevance of the levenshtein distances.

	:param row: each row of the dataframe
	:return row: shortened string i.e. from "coding ltd" to "coding"
	"""
    row = str(row).replace('-', ' ').replace("  ", " ").strip()
    rowsplit = str(row).split(" ")
    for i in rowsplit:
        if i in org_suffixes.org_type_dict.values():
            rowadj = row.replace(i, '').strip()
    try:
        return rowadj
    except:
        return row


def assign_pub_data_to_clusters(df, assigned_file):
    """
	Unmatched members of a cluster are assigned the public data of the highest-confidence matched
	row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
	by the levenshtein distance during the match extraction phase.

	:param df: the main clustered and matched dataframe
	:param assigned_file : file-path to save location
	:return altered df
	"""

    st = set(df['Cluster ID'])
    df.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
    df.reset_index(drop=True, inplace=True)
    tqdm.pandas()
    print("Assigning close matches within clusters...")
    df = df.groupby(['Cluster ID']).progress_apply(get_max_id)
    df.to_csv(assigned_file, index=False)
    return df


def get_max_id(group):
    """
	Used by assign_pub_data_to_clusters(). Takes one entire cluster,
	finds the row with the best confidence score and applies the public data of that row
	to the rest of the rows in that cluster which don't already have matches

	:param group: all rows belonging to one particular cluster
	:return group: the amended cluster to be updated into the main df
	"""
    max_conf_idx = group['Confidence Score'].idxmax()
    for index, row in group.iterrows():
        # If the row is unmatched (has no public org_id):
        if pd.isnull(row.Org_ID):
            group.at[index, 'Org_ID'] = group['Org_ID'][max_conf_idx]
            group.at[index, 'pub_name_adj'] = group['pub_name_adj'][max_conf_idx]
            group.at[index, 'pub_address'] = group['pub_address'][max_conf_idx]
    return group


def calc_match_ratio(row):
    """
	Used in extract_matches() - use fuzzywuzzy to calculate levenshtein distance

	:return ratio: individual levenshtein distance between the public and private org string
	"""
    if pd.notnull(row.priv_name_short) and pd.notnull(row.pub_name_short):
        return fuzz.ratio(row.priv_name_short, row.pub_name_short)


def extract_matches(clustdf, configs, config_dirs, proc_num, proc_type, conf_file_num):
    """
	Import config file containing variable assignments for i.e. char length, match ratio
	Based on the 'cascading' config details, extract matches to new csv

	:return extracts_file: contains dataframe with possible acceptable matches
	"""
    # Round confidence scores to 2dp :
    clustdf['Confidence Score'] = clustdf['Confidence Score'].map(lambda x: round(x, 2))

    # Filter by current match_score:
    clustdf = clustdf[clustdf['leven_dist'] >= configs['processes'][proc_type][proc_num]['min_match_score']]

    # if the earliest process, accept current clustdf as matches, if not (>min):
    if proc_num > min(configs['processes'][proc_type]):
        try:
            # Filter by char count and previous count (if exists):
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() <= configs['processes'][proc_type][proc_num]['char_counts']]
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() > configs['processes'][proc_type][proc_num - 1]['char_counts']]
            # Filter by < 99 as first proc_num includes all lengths leading to duplicates
            clustdf = clustdf[clustdf['leven_dist'] <= 99]
        except:
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() <= configs['processes'][proc_type][proc_num]['char_counts']]
    else:
        if os.path.exists(config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv'):
            # Clear any previous extraction file for this config:
            os.remove(config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv')

    # Add process number to column for calculating stats purposes:
    clustdf['process_num'] = str(proc_num)

    if not os.path.exists(config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv'):
        clustdf.to_csv(config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv',
                       index=False)
        return clustdf
    else:
        extracts_file = pd.read_csv(
            config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv', index_col=None)
        extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
        extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
        extracts_file.to_csv(config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_file_num) + '.csv',
                             index=False)
        return extracts_file


def calc_matching_stats(clustdf, extractdf, config_dirs, conf_file_num, proc_type):
    """
	For each process outlined in the config file, after each process is completed
	extract the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio values

	:return : None
	:output : a short stats file for each config file for manual comparison to see which is better
	"""
    # Remove old stats file if exists and if first iteration over config files:
    if os.path.exists(config_dirs['stats_file'].format(proc_type)):
        if conf_file_num == 1:
            os.remove(config_dirs['stats_file'].format(proc_type))

    statdf = pd.DataFrame(
        columns=['Config_File', 'Total_Matches', 'Percent_Matches', 'Optim_Matches', 'Percent_Precision',
                 'Percent_Recall', 'Leven_Dist_Avg'])
    # Overall matches, including poor quality:
    statdf.at[conf_file_num, 'Config_File'] = conf_file_num
    statdf.at[conf_file_num, 'Total_Matches'] = len(clustdf[pd.notnull(clustdf['Org_ID'])])
    statdf.at[conf_file_num, 'Percent_Matches'] = round(len(clustdf[pd.notnull(clustdf['Org_ID'])]) / len(privdf) * 100,
                                                        2)
    # Overall optimised matches :
    statdf.at[conf_file_num, 'Optim_Matches'] = len(extractdf)
    # Precision - how many of the selected items are relevant to us? (TP/TP+FP)
    statdf.at[conf_file_num, 'Percent_Precision'] = round(len(extractdf) / len(clustdf) * 100, 2)
    # Recall - how many relevant items have been selected from the entire original private data (TP/TP+FN)
    statdf.at[conf_file_num, 'Percent_Recall'] = round(len(extractdf) / len(privdf) * 100, 2)
    statdf.at[conf_file_num, 'Leven_Dist_Avg'] = np.average(extractdf.leven_dist)
    # if statsfile doesnt exist, create it
    if not os.path.exists(config_dirs['stats_file'].format(proc_type)):
        statdf.to_csv(config_dirs['stats_file'].format(proc_type))
    # if it does exist, concat current results with previous
    else:
        main_stat_file = pd.read_csv(config_dirs['stats_file'].format(proc_type), index_col=None)
        main_stat_file = pd.concat([main_stat_file, statdf], ignore_index=True, sort=True)
        main_stat_file.to_csv(config_dirs['stats_file'].format(proc_type), index=False,
                              columns=['Config_File', 'Leven_Dist_Avg', 'Optim_Matches', 'Percent_Matches',
                                       'Percent_Precision', 'Percent_Recall', 'Total_Matches'])


def manual_matching(config_dirs, conf_choice):
    """
	Provides user-input functionality for manual matching based on the extracted records
	:return manual_match_file: extracted file with added column (Y/N/Unsure)
	"""

    manual_match_file = pd.read_csv(
        config_dirs['extract_matches_file'].format(proc_type) + '_' + str(conf_choice) + '.csv', index_col=None)
    manual_match_file['Manual_Match'] = ''

    choices = ['n', 'na']
    choice = input("\nMatching name only or name and address? (N / NA):")
    while choice.lower() not in choices:
        choice = input("\nMatching name only or name and address? (N / NA):")

    # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
    for index, row in manual_match_file.sample(frac=1).iterrows():
        if choice.lower() == 'n':
            print("\nPrivate name: " + str(row.priv_name_adj))
            print("\nPublic name: " + str(row.pub_name_adj))
            print("\nLevenshtein distance: " + str(row.leven_dist))
        else:
            print("\nPrivate name: " + str(row.priv_name_adj))
            print("Private address: " + str(row.priv_address))
            print("\nPublic name: " + str(row.pub_name_adj))
            print("Public address: " + str(row.pub_address))
            print("\nLevenshtein distance (names): " + str(row.leven_dist))

        match_options = ["y", "n", "u", "f"]
        match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
        while match.lower() not in match_options:
            match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")

        if str(match).lower() != "f":
            manual_match_file.at[index, 'Manual_Match'] = str(match).capitalize()
            continue
        else:
            break

    manual_match_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)

    print("Saving...")
    manual_match_file.to_csv(config_dirs['manual_matches_file'].format(proc_type) + '_' + str(conf_choice) + '.csv',
                             index=False,
                             columns=['Cluster ID', 'Confidence Score', 'Org_ID', 'id', 'leven_dist', 'org_name',
                                      'priv_address',
                                      'priv_name', 'priv_name_adj', 'process_num', 'pub_address', 'pub_name_adj',
                                      'Manual_Match'])
    return manual_match_file, choice


def convert_to_training(config_dirs, man_matched):
    """
	Converts the manually matched dataframe into a training file for dedupe
	:return : None
	:output : training.json training file
	"""

    # Filter for matched entries
    man_matched = man_matched[pd.notnull(man_matched['Manual_Match'])]
    manualdict = {}
    manualdict['distinct'] = []
    manualdict['match'] = []

    # For each row in in the manual matches df, create a sub-dict to be
    # appended to manualdict
    for index, row in man_matched.iterrows():
        new_data = {"__class__": "tuple",
                    "__value__": [
                        {
                            "priv_name_adj": str(row.priv_name_adj),
                            "priv_address": str(row.priv_address)
                        },
                        {
                            "priv_name_adj": str(row.pub_name_adj),
                            "priv_address": str(row.pub_address)
                        }
                    ]}

        # If the row was a match or not a match, append to
        # either the match key or the distinct key, respectively:
        if row.Manual_Match == 'Y':
            manualdict['match'].append(new_data)
        elif row.Manual_Match == 'N':
            manualdict['distinct'].append(new_data)
        # If row was 'unsure'd, ignore it as it doesn't contribute to training data
        else:
            continue
    # Write dict to training file backup.
    # 'w+' allows writing, and + creates if doesn't exist.
    with open(config_dirs['manual_matching_train_backup'], 'w+') as outfile:
        json.dump(manualdict, outfile)


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


if __name__ == '__main__':
    in_args = get_input_args()
    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None

    # Define config file variables and related arguments
    config_path = Path('./Config_Files')
    config_dirs = config_dirs.dirs["dirs"]

    # Ignores config_dirs - convention is <num>_config.py
    pyfiles = "*_config.py"

    # If public data doesn't exist:
    if not os.path.exists(config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name)):
        while true:
            choice = input("Public data not found, load from database? (y/n)")
            if choice.lower() == 'y':
                # Load public data
                data_source = "spaziodati.sd_sample"
                qu = construct_query(data_source)
                df = fetch_data(qu)
                df.to_csv(config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name))
            else:
                break

    if not os.path.exists(config_dirs['raw_dir'] + config_dirs['raw_priv_data'].format(in_args.priv_raw_name)):
        # Need to find private/source data table
        pass



    try:
        # For each config file read it and convert to dictionary for accessing
        for conf_file in config_path.glob(pyfiles):
            with open(conf_file) as config_file:
                file_contents = []
                file_contents.append(config_file.read())

                # Convert list to dictionary
                configs = ast.literal_eval(file_contents[0])
                conf_file_num = int(conf_file.name[0])

                # Clean public and private datasets for linking
                # private df needed in memory for stats
                privdf = clean_private_data(config_dirs)
                if not in_args.recycle:
                    pubdf = clean_public_data(config_dirs)

                # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                for proc_type in configs['processes']:

                    # Only run through the module once per initiation. Either to get the initial training file
                    # or to recycle it and get better matches.

                    if in_args.recycle == configs['processes'][proc_type][min(configs['processes'][proc_type].keys())]['recycle_phase']:
                        print("Process type :" + str(proc_type) + "\nConfig file: " + str(conf_file_num))
                        # Check if proc_type output directory exists, if not create it and training directories:
                        proc_type_dir = config_dirs['proc_type_dir'].format(proc_type)
                        if not os.path.exists(proc_type_dir):
                            os.makedirs(proc_type_dir)
                            os.makedirs(config_dirs['confirmed_matches_dir'].format(proc_type))
                            os.makedirs(config_dirs['deduped_dir'].format(proc_type))
                        if not os.path.exists(config_dirs['proc_type_train_dir'].format(proc_type)):
                            os.makedirs(config_dirs['proc_type_train_dir'].format(proc_type))
                        if not os.path.exists(config_dirs['proc_type_train_clust_dir'].format(proc_type)):
                            os.makedirs(config_dirs['proc_type_train_clust_dir'].format(proc_type))
                        if not os.path.exists(config_dirs['proc_type_train_match_dir'].format(proc_type)):
                            os.makedirs(config_dirs['proc_type_train_match_dir'].format(proc_type))
                        if not os.path.exists(config_dirs['proc_type_matches_dir'].format(proc_type)):
                            os.makedirs(config_dirs['proc_type_matches_dir'].format(proc_type))
                        if not os.path.exists(config_dirs['backups_dir'].format(proc_type)):
                            os.makedirs(config_dirs['backups_dir'].format(proc_type))

                        # Iterate over each process number in the config file
                        for proc_num in configs['processes'][proc_type]:
                            # Get first process from config file
                            main_proc = min(configs['processes'][proc_type].keys())

                            # Define data types for clustered file. Enables faster loading.
                            clustdtype = {'Cluster ID': np.int, 'Confidence Score': np.float,
                                          'id': np.int, 'priv_name': np.str, 'priv_address': np.str,
                                          'priv_name_adj': np.str, 'Org_ID': np.str, 'pub_name_adj': np.str,
                                          'pub_address': np.str, 'priv_name_short': np.str, 'pub_name_short': np.str,
                                          'leven_dist': np.int}
                            # Run dedupe for matching and calculate related stats for comparison
                            if not os.path.exists(config_dirs['assigned_output_file'].format(proc_type)):
                                # if 'dedupe_field_names' in proc_fields:
                                dedupe_match_cluster(config_dirs, configs, proc_type, proc_num)

                                clustdf = pd.read_csv(config_dirs["cluster_output_file"].format(proc_type),
                                                      index_col=None,
                                                      dtype=clustdtype)

                                # Copy public data to high-confidence cluster records
                                clustdf = assign_pub_data_to_clusters(clustdf,
                                                                      config_dirs['assigned_output_file'].format(
                                                                          proc_type))

                                # Remove company suffixes for more relevant levenshtein distance calculation
                                clustdf['priv_name_short'] = clustdf.priv_name_adj.apply(shorten_name)

                                clustdf['pub_name_short'] = clustdf.pub_name_adj.apply(shorten_name)

                                # Add column containing levenshtein distance between the matched public & private org names
                                if 'leven_dist' not in clustdf.columns:
                                    clustdf['leven_dist'] = clustdf.apply(calc_match_ratio, axis=1)

                                clustdf.to_csv(config_dirs["assigned_output_file"].format(proc_type), index=False)
                            else:
                                clustdf = pd.read_csv(config_dirs["assigned_output_file"].format(proc_type),
                                                      index_col=None, dtype=clustdtype, usecols=clustdtype.keys())

                            # Adds leven_dist column and extract matches based on config process criteria:
                            extracts_file = extract_matches(clustdf, configs, config_dirs, proc_num, proc_type,
                                                            conf_file_num)
                        break
                    else:
                        continue
                # Output stats file:
                calc_matching_stats(clustdf, extracts_file, config_dirs, conf_file_num, proc_type)

    except StopIteration:
        # End program if no more config files found
        print("Done")

    conf_choice = input(
        ("\nReview Outputs/{0}/Extracted_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
            proc_type))

    # Takes user through the extracted_matches file, to confirm the matches and outputs to separate csv
    man_matched, choice = manual_matching(config_dirs, conf_choice)
    # Convert manual matches to JSON training file.
    man_matched = pd.read_csv(config_dirs['manual_matches_file'].format(proc_type) + '_' + str(conf_choice) + '.csv',
                              usecols=['Manual_Match', 'priv_name_adj', 'priv_address', 'pub_name_adj', 'pub_address'])

    # If initial round of processing, create manual training file:
    if in_args.recycle == False:
        convert_to_training(config_dirs, man_matched)

    # Filter manual matches file and output to separate csv as confirmed matches
    confirmed_matches = man_matched[man_matched['Manual_Match'] == 'Y']
    confirmed_matches.to_csv(config_dirs['confirmed_matches_file'].format(proc_type), index=False)

    # Add confirmed matches to database depending on whether manual matching was name only or name and address
    if choice.lower() == 'n':
        add_data_to_table("spaziodati.confirmed_nameonly_matches")

    elif choice.lower() == 'na':
        add_data_to_table("spaziodati.confirmed_nameaddress_matches")

    print("Done.")
