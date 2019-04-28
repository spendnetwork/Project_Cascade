import subprocess
import time
import logging
from shutil import copyfile
import chwrapper
import math
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
import pandas as pd
import pdb
from fuzzywuzzy import fuzz

load_dotenv(find_dotenv())
companieshouse_key = os.environ.get("API_KEY2")
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

def matching(configs, proc_type, directories, regiondir, runfilemods, privdf, in_args, proc_num, df_dtypes):
    if not os.path.exists(directories['match_output_file'].format(regiondir, proc_type)):
        runfilemods.data_matching.companies_house_matching(privdf, directories, regiondir, proc_type)


    runfilemods.data_processing.clean_matched_data(directories, regiondir, proc_type)
    # Run dedupe for matching and calculate related stats for comparison

    if not os.path.exists(directories["cluster_output_file"].format(regiondir, proc_type)):
        runfilemods.data_matching.dedupe_match_cluster(regiondir, directories, configs, proc_type, proc_num, in_args)

    if not os.path.exists(directories['assigned_output_file'].format(regiondir, proc_type)):
        clust_df = pd.read_csv(directories["cluster_output_file"].format(regiondir, proc_type), index_col=None)

        # Adds leven_dist column and extract matches based on config process criteria:
        clust_df = runfilemods.data_processing.add_lev_dist(clust_df, directories["assigned_output_file"].format(regiondir, proc_type))

    else:

        clust_df = pd.read_csv(directories["assigned_output_file"].format(regiondir, proc_type), dtype=df_dtypes, index_col=None)
    return clust_df



def companiesHouseMatching(df,directories,regiondir,proc_type):
    """
    Lookup company name via Companies House API and return company number
    :param df: pandas dataframe containing the organisation name
    :return df: Amended dataframe containing additional company information
    """

    s = chwrapper.Search(access_token=companieshouse_key)
    # Tried matching to priv_name_adj but results were poor. Keeping adj/short column for clustering...
    org_strings = df['priv_name']
    ch_name_dict = {}
    ch_id_dict = {}
    ch_addr_dict = {}
    chunk_propn = 0

    # Split org_string array into multiple arrays.
    # api states max batch of 600...
    # array_split doesn't have to have equal batch sizes.
    for chunk in np.array_split(org_strings,
                                math.ceil(len(org_strings) / 500), axis=0):

        print("\nProcessing companies house batch of size: " + str(len(chunk)))

        # For each org_string in the sub-array of org_strings
        # pull org data from companies house
        for word in tqdm(chunk):
            response = s.search_companies(word)
            if response.status_code == 200:
                dict = response.json()
                # response.json() returns a nested dict with complete org info

                name = ''
                chId = 0
                chAddr = ''
                dict = dict['items']
                for i in range(len(dict)):
                    if fuzz.ratio(word, dict[i]['title']) > fuzz.ratio(word, name):
                        name = dict[i]['title']
                        chId = dict[i]['company_number']
                        chAddr = dict[i]['address_snippet']
                    else:
                        continue

                ch_name_dict[word] = name
                ch_id_dict[name] = chId
                ch_addr_dict[name] = chAddr
                ch_name_dict.update(ch_name_dict)
                ch_id_dict.update(ch_id_dict)
                ch_addr_dict.update(ch_addr_dict)

            elif response.status_code == 404:
                logger.debug("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
            elif response.status_code == 429:
                logger.debug("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
                logger.debug("Waiting...")
                time.sleep(60)
                s = chwrapper.Search(access_token=companieshouse_key)
            else:
                logger.error("Error requesting CH data: %s %s",
                             response.status_code, response.reason)
        chunk_propn += int(len(chunk))
        print("\nProgress: " + str(chunk_propn) + " of " +
              str(len(org_strings)))

        df['CH_name'] = df['priv_name'].map(ch_name_dict)
        df['CH_id'] = df['CH_name'].map(ch_id_dict)
        df['CH_address'] = df['CH_name'].map(ch_addr_dict)

    # Remove error matches
    df = df.dropna(axis=0, subset=['CH_name'])

    df.to_csv(directories['match_output_file'].format(regiondir, proc_type),index=False)


def dedupeMatchCluster(regiondir, directories, config_files, proc_type, proc_num, in_args):
    """
	Deduping - first the public and private data are matched using dedupes csvlink,
	then the matched file is put into clusters
    :param pub_file:
    :param 	:param directories: file/folder locations
	:param  config_files: the main config files
	:param proc_type: the 'type' of the process (Name, Name & Address)
	:param proc_num: the individual process within the config file
	:return None
	:output : matched output file
	:output : matched and clustered output file
	"""

    priv_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['private_data']

    train = ['--skip_training' if in_args.training else '']

    # Clustering:
    if not os.path.exists(directories['cluster_output_file'].format(regiondir, proc_type)):
        # Copy training file from first clustering session if recycle mode

        print("Starting clustering...")
        cmd = ['python csvdedupe.py '
               + directories['match_output_file'].format(regiondir, proc_type) + ' '
               + ' --field_names ' + ' '.join(priv_fields) + ' '
               + str(train[0])
               + ' --training_file ' + directories['cluster_training_file'].format(regiondir, proc_type)
               + ' --output_file ' + directories['cluster_output_file'].format(regiondir, proc_type)]
        p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
        p.wait()  # wait for subprocess to finish

    else:
        pass


def extractMatches(regiondir, clustdf, config_files, directories, proc_num, proc_type, conf_file_num, in_args):
    """
	Import config file containing variable assignments for i.e. char length, match ratio
	Based on the 'cascading' config details, extract matches to new csv

	:return extracts_file: contains dataframe with possible acceptable matches
	"""


    levendist = str('leven_dist_N')

    # Round confidence scores to 2dp :

    clustdf['Confidence Score'] = clustdf['Confidence Score'].map(lambda x: round(x, 2))

    # Filter by current match_score:
    clustdf = clustdf[clustdf[levendist] >= config_files['processes'][proc_type][proc_num]['min_match_score']]

    # if the earliest process, accept current clustdf as matches, if not (>min):
    if proc_num > min(config_files['processes'][proc_type]):
        try:
            # Filter by char count and previous count (if exists):
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() > config_files['processes'][proc_type][proc_num - 1]['char_counts']]
            # Filter by < 99 as first proc_num includes all lengths leading to duplicates
            clustdf = clustdf[clustdf[levendist] <= 99]
        except:
            clustdf = clustdf[
                clustdf.priv_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
    else:
        if os.path.exists(directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv'):
            # Clear any previous extraction file for this config:
            os.remove(directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv')

    # Add process number to column for calculating stats purposes:
    clustdf['process_num'] = str(proc_num)

    if not os.path.exists(directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv'):
        clustdf.to_csv(directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv',
                       index=False)
        return clustdf
    else:
        extracts_file = pd.read_csv(
            directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv', index_col=None)
        extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
        extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
        extracts_file.to_csv(directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(conf_file_num) + '.csv',
                             index=False)
        return extracts_file


def manual_matching(regiondir, directories, best_config, proc_type, in_args):
    """
	Provides user-input functionality for manual matching based on the extracted records
	:return manual_match_file: extracted file with added column (Y/N/Unsure)
	"""

    manual_match_file = pd.read_csv(
        directories['extract_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv', index_col=None)
    manual_match_file['Manual_Match_N'] = ''

    # Automatically confirm rows with leven dist of 100
    for index, row in manual_match_file.iterrows():
        if row.leven_dist_N == 100:
            manual_match_file.at[index, 'Manual_Match_N'] = str('Y')

    if in_args.terminal_matching:
        # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
        for index, row in manual_match_file.sample(frac=1).iterrows():
            print("\nPrivate name: " + str(row.priv_name_adj))
            print("\nPublic name: " + str(row.pub_name_adj))
            print("\nLevenshtein distance: " + str(row.leven_dist_N))
            match_options = ["y", "n", "u", "f"]
            match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
            while match.lower() not in match_options:
                match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")

            if str(match).lower() != "f":
                manual_match_file.at[index, 'Manual_Match_N'] = str(match).capitalize()
                continue
            else:
                break

        manual_match_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)

        print("Saving...")
        manual_match_file.to_csv(directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
                                 index=False,columns=['priv_name','CH_name','Manual_Match_N','about_or_contact_text','company_url','home_page_text','CH_id','CH_address','priv_name_short','CH_name_short','leven_dist_N'])
        return manual_match_file

    else:
        manual_match_file.to_csv(directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
                                 index=False, columns=['priv_name','CH_name','Manual_Match_N','about_or_contact_text','company_url','home_page_text','CH_id','CH_address','priv_name_short','CH_name_short','leven_dist_N'])

        if not in_args.upload_to_db:
            print("\nIf required, please perform manual matching process in {} and then run 'python runfile.py --convert_training --upload_to_db".format(
            directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv'))