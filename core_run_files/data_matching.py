import pandas as pd
import os
import subprocess
import numpy as np
from shutil import copyfile


def dedupe_matchTEST(src_file, reg_file, region_dir, directories, config_files, proc_type, proc_num, in_args):
    """
	Deduping - first the registry and source data are matched using dedupes csvlink,
	then the matched file is put into clusters
	:param directories: file/folder locations
	:param  config_files: the main config files
	:param proc_type: the 'type' of the process (Name, Name & Address)
	:param proc_num: the individual process within the config file
	:return None
	:output : matched output file
	:output : matched and clustered output file
	"""
    src_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['source_data']
    reg_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['registry_data']

    train = ['--skip_training' if in_args.training else '']
    # Matching:
    if not os.path.exists(directories['match_output_file'].format(region_dir, proc_type)):
        if in_args.recycle:
            # Copy manual matching file over to build on for clustering
            copyfile(directories['manual_matching_train_backup'].format(region_dir), directories['manual_training_file'].format(region_dir, proc_type))

        # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
        if os.path.exists('./learned_settings'):
            os.remove('./learned_settings')
        print("Starting matching...")

        cmd = ['csvlink '
               + str(src_file) + ' '
               + str(reg_file)
               + ' --field_names_1 ' + ' '.join(src_fields)
               + ' --field_names_2 ' + ' '.join(reg_fields)
               + ' --training_file ' + directories['manual_training_file'].format(region_dir, proc_type)
               + ' --output_file ' + directories['match_output_file'].format(region_dir, proc_type) + ' '
               + str(train[0])
               ]
        p = subprocess.Popen(cmd, shell=True)

        p.wait()
        df = pd.read_csv(directories['match_output_file'].format(region_dir, proc_type),
                         usecols=['id', 'src_name', 'src_address', 'src_name_adj', 'src_address_adj', 'reg_id', 'reg_name',
                                  'reg_name_adj','reg_address_adj',
                                  'reg_address', 'reg_address_adj', 'srcjoinfields', 'regjoinfields'],
                         dtype={'id': np.str, 'src_name': np.str, 'src_address': np.str, 'src_name_adj': np.str, 'src_address_adj': np.str,
                                'reg_id': np.str, 'reg_name': np.str, 'reg_name_adj': np.str, 'reg_address': np.str, 'reg_address_adj': np.str, 'srcjoinfields':np.str, 'regjoinfields':np.str})
        df = df[pd.notnull(df['src_name'])]
        df.to_csv(directories['match_output_file'].format(region_dir, proc_type), index=False)


def dedupe_match_cluster(src_file, reg_file, region_dir, directories, config_files, proc_type, proc_num, in_args):
    """
	Deduping - first the registry and source data are matched using dedupes csvlink,
	then the matched file is put into clusters
    :param reg_file:
    :param src_file:
	:param directories: file/folder locations
	:param  config_files: the main config files
	:param proc_type: the 'type' of the process (Name, Name & Address)
	:param proc_num: the individual process within the config file
	:return None
	:output : matched output file
	:output : matched and clustered output file
	"""

    src_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['source_data']
    reg_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['registry_data']

    train = ['--skip_training' if in_args.training else '']
    # Matching:
    if not os.path.exists(directories['match_output_file'].format(region_dir, proc_type)):
        if in_args.recycle:
            # Copy manual matching file over to build on for clustering
            copyfile(directories['manual_matching_train_backup'].format(region_dir), directories['manual_training_file'].format(region_dir, proc_type))

        # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
        if os.path.exists('./learned_settings'):
            os.remove('./learned_settings')
        print("Starting matching...")

        cmd = ['csvlink '
               + str(src_file) + ' '
               + str(reg_file)
               + ' --field_names_1 ' + ' '.join(src_fields)
               + ' --field_names_2 ' + ' '.join(reg_fields)
               + ' --training_file ' + directories['manual_training_file'].format(region_dir, proc_type)
               + ' --output_file ' + directories['match_output_file'].format(region_dir, proc_type) + ' '
               + str(train[0])
               ]
        p = subprocess.Popen(cmd, shell=True)

        p.wait()
        df = pd.read_csv(directories['match_output_file'].format(region_dir, proc_type),
                         usecols=['id', 'src_name', 'src_address', 'src_name_adj', 'src_address_adj', 'reg_id', 'reg_name',
                                  'reg_name_adj', 'reg_address_adj',
                                  'reg_address', 'reg_address_adj', 'srcjoinfields', 'regjoinfields'],
                         dtype={'id': np.str, 'src_name': np.str, 'src_address': np.str, 'src_name_adj': np.str, 'src_address_adj': np.str,
                                'reg_id': np.str, 'reg_name': np.str, 'reg_name_adj': np.str, 'reg_address': np.str, 'reg_address_adj': np.str, 'srcjoinfields':np.str, 'regjoinfields':np.str})
        df = df[pd.notnull(df['src_name'])]
        df.to_csv(directories['match_output_file'].format(region_dir, proc_type), index=False)

    # Clustering:
    if not os.path.exists(directories['cluster_output_file'].format(region_dir, proc_type)):
        # Copy training file from first clustering session if recycle mode
        if in_args.recycle:
            copyfile(directories['cluster_training_backup'].format(region_dir), directories['cluster_training_file'].format(region_dir, proc_type))

        print("Starting clustering...")
        cmd = ['python csvdedupe.py '
               + directories['match_output_file'].format(region_dir, proc_type) + ' '
               + ' --field_names ' + ' '.join(src_fields) + ' '
               + str(train[0])
               + ' --training_file ' + directories['cluster_training_file'].format(region_dir, proc_type)
               + ' --output_file ' + directories['cluster_output_file'].format(region_dir, proc_type)]
        p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
        p.wait()  # wait for subprocess to finish

        if not in_args.recycle:
            # Copy training file to backup, so it can be found and copied into recycle phase clustering
            copyfile(directories['cluster_training_file'].format(region_dir, proc_type), directories['cluster_training_backup'].format(region_dir))
    else:
        pass


def extract_matches(region_dir, clustdf, config_files, directories, proc_num, proc_type, conf_file_num, in_args):
    """
	Import config file containing variable assignments for i.e. char length, match ratio
	Based on the 'cascading' config details, verify matches to new csv

	:return extracts_file: contains dataframe with possible acceptable matches
	"""

    if in_args.recycle:
        levendist = str('leven_dist_NA')
    else:
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
                clustdf.src_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
            clustdf = clustdf[
                clustdf.src_name_short.str.len() > config_files['processes'][proc_type][proc_num - 1]['char_counts']]
            # Filter by < 99 as first proc_num includes all lengths leading to duplicates
            clustdf = clustdf[clustdf[levendist] <= 99]
        except:
            clustdf = clustdf[
                clustdf.src_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
    else:
        if os.path.exists(directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv'):
            # Clear any previous extraction file for this config:
            os.remove(directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv')

    # Add process number to column for calculating stats purposes:
    clustdf['process_num'] = str(proc_num)

    if not os.path.exists(directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv'):
        clustdf.to_csv(directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv',
                       index=False)
        return clustdf
    else:
        extracts_file = pd.read_csv(
            directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv', index_col=None)
        extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
        extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
        extracts_file.to_csv(directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv',
                             index=False)
        return extracts_file


def manual_matching(region_dir, directories, best_config, proc_type, in_args):
    """
	Provides user-input functionality for manual matching based on the extracted records
	:return manual_match_file: extracted file with added column (Y/N/Unsure)
	"""

    manual_match_file = pd.read_csv(
        directories['extract_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv', index_col=None)
    manual_match_file['Manual_Match_N'] = ''
    manual_match_file['Manual_Match_NA'] = ''

    # Automatically confirm rows with leven dist of 100
    for index, row in manual_match_file.iterrows():
        if row.leven_dist_N == 100:
            manual_match_file.at[index, 'Manual_Match_N'] = str('Y')
        if row.leven_dist_NA == 100:
            manual_match_file.at[index, 'Manual_Match_NA'] = str('Y')

    if in_args.terminal_matching:
        choices = ['n', 'na']
        choice = input("\nMatching name only or name and address? (N / NA):")
        while choice.lower() not in choices:
            choice = input("\nMatching name only or name and address? (N / NA):")

        # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
        for index, row in manual_match_file.sample(frac=1).iterrows():
            if choice.lower() == 'n':
                print("\nsource name: " + str(row.src_name_adj))
                print("\nRegistry name: " + str(row.reg_name_adj))
                print("\nLevenshtein distance: " + str(row.leven_dist_N))
            else:
                print("\nsource name: " + str(row.src_name_adj))
                print("source address: " + str(row.src_address_adj))
                print("\nRegistry name: " + str(row.reg_name_adj))
                print("Registry address: " + str(row.reg_address_adj))
                print("\nLevenshtein distance : " + str(row.leven_dist_NA))

            match_options = ["y", "n", "u", "f"]
            match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
            while match.lower() not in match_options:
                match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")

            if str(match).lower() != "f":
                manual_match_file.at[index, 'Manual_Match_N'] = str(match).capitalize()
                # Need to add in NA version ? Might just remove terminal matching altogether...
                continue
            else:
                break

        manual_match_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)

        print("Saving...")
        manual_match_file.to_csv(directories['manual_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv',
                                 index=False,
                                 # columns=['reg_id', 'id', 'reg_name',
                                 #          'src_name','reg_address', 'src_address', 'leven_dist_N', 'leven_dist_NA','Manual_Match_N','Manual_Match_NA'])
                                columns = ['Cluster ID', 'leven_dist_N', 'leven_dist_NA', 'reg_id', 'id', 'reg_name', 'reg_name_adj',
                                           'reg_address', 'src_name', 'src_name_adj', 'src_address', 'src_address_adj', 'reg_address_adj',
                                           'Manual_Match_N', 'Manual_Match_NA', 'srcjoinfields', 'regjoinfields'])
        return manual_match_file

    else:
        manual_match_file.to_csv(directories['manual_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv',
                                 index=False,
                                 # columns=['reg_id', 'id', 'reg_name',
                                 #          'src_name', 'reg_address', 'src_address', 'leven_dist_N', 'leven_dist_NA',
                                 #          'Manual_Match_N', 'Manual_Match_NA'])
                                 columns=['Cluster ID', 'leven_dist_N', 'leven_dist_NA', 'reg_id', 'id', 'reg_name', 'reg_name_adj',
                                          'reg_address','src_name', 'src_name_adj', 'src_address', 'src_address_adj', 'reg_address_adj', 'Manual_Match_N','Manual_Match_NA', 'srcjoinfields', 'regjoinfields'])
        if not in_args.recycle:
            if not in_args.upload_to_db:
                print("\nIf required, please perform manual matching process in {} and then run 'python runfile.py --convert_training --upload_to_db".format(
                directories['manual_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv'))
        else:
            if not in_args.upload_to_db:
                print("\nIf required, please perform manual matching process in {} and then run 'python runfile.py --recycle --upload_to_db".format(
                    directories['manual_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv'))