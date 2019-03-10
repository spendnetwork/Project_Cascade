import pandas as pd
import os
import subprocess
import numpy as np
import sys
from shutil import copyfile


def dedupe_match_cluster(dirs, configs, proc_type, proc_num, in_args):
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

    train = ['--skip_training' if in_args.training else '']
    # Matching:
    if not os.path.exists(dirs['match_output_file'].format(proc_type)):
        if in_args.recycle:
            # Copy manual matching file over to build on for clustering
            copyfile(dirs['manual_matching_train_backup'], dirs['manual_training_file'].format(proc_type))

        # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
        if os.path.exists('./learned_settings'):
            os.remove('./learned_settings')
        print("Starting matching...")

        cmd = ['csvlink '
               + str(priv_file).format(in_args.priv_adj_name) + ' '
               + str(pub_file).format(in_args.pub_adj_name)
               + ' --field_names_1 ' + ' '.join(priv_fields)
               + ' --field_names_2 ' + ' '.join(pub_fields)
               + ' --training_file ' + dirs['manual_training_file'].format(proc_type)
               + ' --output_file ' + dirs['match_output_file'].format(proc_type) + ' '
               + str(train[0])
               ]
        p = subprocess.Popen(cmd, shell=True)

        p.wait()
        df = pd.read_csv(dirs['match_output_file'].format(proc_type),
                         usecols=['id', 'priv_name', 'priv_address', 'priv_name_adj', 'Org_ID', 'org_name',
                                  'pub_name_adj',
                                  'pub_address'],
                         dtype={'id': np.str, 'priv_name': np.str, 'priv_address': np.str, 'priv_name_adj': np.str,
                                'Org_ID': np.str, 'org_name': np.str, 'pub_name_adj': np.str, 'pub_address': np.str})
        df = df[pd.notnull(df['priv_name'])]
        df.to_csv(dirs['match_output_file'].format(proc_type), index=False)

    # Clustering:
    if not os.path.exists(dirs['cluster_output_file'].format(proc_type)):
        # Copy training file from first clustering session if recycle mode
        if in_args.recycle:
            copyfile(dirs['cluster_training_backup'], dirs['cluster_training_file'].format(proc_type))

        print("Starting clustering...")
        cmd = ['python csvdedupe.py '
               + dirs['match_output_file'].format(proc_type) + ' '
               + ' --field_names ' + ' '.join(priv_fields) + ' '
               + str(train[0])
               + ' --training_file ' + dirs['cluster_training_file'].format(proc_type)
               + ' --output_file ' + dirs['cluster_output_file'].format(proc_type)]
        p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
        p.wait()  # wait for subprocess to finish

        if not in_args.recycle:
            # Copy training file to backup, so it can be found and copied into recycle phase clustering
            copyfile(dirs['cluster_training_file'].format(proc_type), dirs['cluster_training_backup'])
    else:
        pass


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


def manual_matching(config_dirs, best_config, proc_type, in_args):
    """
	Provides user-input functionality for manual matching based on the extracted records
	:return manual_match_file: extracted file with added column (Y/N/Unsure)
	"""

    manual_match_file = pd.read_csv(
        config_dirs['extract_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv', index_col=None)
    manual_match_file['Manual_Match'] = ''

    # Automatically confirm rows with leven dist of 100
    for index, row in manual_match_file.iterrows():
        if row.leven_dist == 100:
            manual_match_file.at[index, 'Manual_Match'] = str('Y')

    if in_args.terminal_matching:
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
        manual_match_file.to_csv(config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
                                 index=False,
                                 # columns=['Cluster ID', 'Confidence Score', 'Org_ID', 'id', 'leven_dist', 'org_name',
                                 #          'priv_address',
                                 #          'priv_name', 'priv_name_adj', 'process_num', 'pub_address', 'pub_name_adj',
                                 #          'Manual_Match'])
                                 columns=['Cluster ID', 'leven_dist', 'Org_ID', 'id', 'org_name', 'pub_name_adj', 'pub_address',
                                          'priv_name', 'priv_name_adj', 'priv_address', 'Manual_Match'])
        return manual_match_file

    else:
        manual_match_file.to_csv(config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
                                 index=False,
                                 columns=['Cluster ID', 'leven_dist', 'Org_ID', 'id', 'org_name', 'pub_name_adj',
                                          'pub_address','priv_name', 'priv_name_adj', 'priv_address', 'Manual_Match'])

        print("\nIf required, please perform manual matching process in {} and then run 'python run.py --convert_training --upload_to_db".format(
            config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv'))