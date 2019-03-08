import sys
import argparse
import pandas as pd
import os
import numpy as np
import ast
from run_files import setup, data_matching, db_calls, convert_training, data_processing, data_analysis
from pathlib import Path
from Config_Files import config_dirs
import pdb



def get_input_args():
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    function_map = {
        'convert_to_training' : convert_training.convert_to_training,
        'add_data_to_table' : db_calls.add_data_to_table
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('--priv_raw_name', default='private_data.csv', type=str,
                        help='Set raw private/source datafile name')
    parser.add_argument('--pub_raw_name', default='public_data.csv', type=str, help='Set raw public datafile name')
    parser.add_argument('--priv_adj_name', default='priv_data_adj.csv', type=str,
                        help='Set cleaned private/source datafile name')
    parser.add_argument('--pub_adj_name', default='pub_data_adj.csv', type=str, help='Set cleaned public datafile name')
    parser.add_argument('--recycle', action='store_true', help='Recycle the manual training data')
    parser.add_argument('--training', action='store_false', help='Modify/contribute to the training data')
    parser.add_argument('--config_review', action='store_true', help='Manually review/choose best config file results')
    parser.add_argument('--terminal_matching', action='store_true', help='Perform manual matching in terminal')
    parser.add_argument('--convert_training', action='store_true', help='Convert confirmed matches to training file for recycle phase')
    parser.add_argument('--upload_to_db', action='store_true' , help='Add confirmed matches to database')
    args = parser.parse_args()

    # If the clustering training file does not exist (therefore the matching train file too as this is created before the former)
    # Force an error and prompt user to add the training flag
    if args.training == True and not os.path.exists(os.getcwd() + "/Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json"):
        parser.error("Dedupe training files do not exist, please try 'python run.py --training' to begin training process")

    return args


if __name__ == '__main__':

    # main()
    in_args = get_input_args()
    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None
    # Define config file variables and related arguments
    config_path = Path('./Config_Files')
    config_dirs = config_dirs.dirs["dirs"]

    # Ignores config_dirs - convention is <num>_config.py
    pyfiles = "*_config.py"

    # If public/registry data file doesn't exist, pull from database
    db_calls.check_data_exists(config_dirs,in_args)

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
                privdf = data_processing.clean_private_data(config_dirs, in_args)
                if not in_args.recycle:
                    pubdf = data_processing.clean_public_data(config_dirs, in_args)

                # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                for proc_type in configs['processes']:
                    if in_args.recycle == configs['processes'][proc_type][min(configs['processes'][proc_type].keys())][
                        'recycle_phase']:

                        # Create working directories if don't exist
                        setup.setup_dirs(config_dirs,proc_type)

                        # Iterate over each process number in the config file
                        for proc_num in configs['processes'][proc_type]:
                            # Get first process from config file
                            main_proc = min(configs['processes'][proc_type].keys())

                            # Define data types for clustered file. Enables faster loading.
                            clustdtype = {'Cluster ID': np.int, 'Confidence Score': np.float,
                                          'id': np.int, 'priv_name': np.str, 'priv_address': np.str,
                                          'priv_name_adj': np.str, 'Org_ID': np.str, 'pub_name_adj': np.str,
                                          'pub_address': np.str, 'priv_name_short': np.str, 'pub_name_short': np.str,
                                          'leven_dist': np.int, 'org_name': np.str}

                            # Run dedupe for matching and calculate related stats for comparison
                            if not os.path.exists(config_dirs['assigned_output_file'].format(proc_type)):

                                data_matching.dedupe_match_cluster(config_dirs, configs, proc_type, proc_num, in_args)

                                clust_df = pd.read_csv(config_dirs["cluster_output_file"].format(proc_type),index_col=None,dtype=clustdtype)

                                # Copy public data to high-confidence cluster records
                                clust_df = data_processing.assign_pub_data_to_clusters(clust_df,config_dirs['assigned_output_file'].format(proc_type))

                                # Adds leven_dist column and extract matches based on config process criteria:
                                clust_df = data_processing.add_lev_dist(clust_df, config_dirs["assigned_output_file"].format(proc_type))

                            else:

                                clust_df = pd.read_csv(config_dirs["assigned_output_file"].format(proc_type),
                                                       index_col=None, dtype=clustdtype, usecols=clustdtype.keys())

                            extracts_file = data_matching.extract_matches(clust_df, configs, config_dirs, proc_num, proc_type,
                                                            conf_file_num)
                        break
                    else:
                        continue
                # Output stats file:
                stat_file = data_analysis.calc_matching_stats(clust_df, extracts_file, config_dirs, conf_file_num, proc_type, privdf)

    except StopIteration:
        # Continue if no more config files found
        print("Done")


    # For each process type (eg: Name & Add, Name only) outlined in the configs file:
    for proc_type in configs['processes']:

        # If recycle arg matches the recycle variable in the proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if in_args.recycle == configs['processes'][proc_type][min(configs['processes'][proc_type].keys())]['recycle_phase']:

            # If user has used --config_review flag, set best_config variable based on manual review of stats file...
            if in_args.config_review:
                best_config = input(
                    ("\nReview Outputs/{0}/Extracted_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
                        proc_type))
            else:
                # ...otherwise pick best config_file based on stats file (max leven dist avg):
                max_lev = stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
                best_config = stat_file.at[max_lev, 'Config_File']

            data_matching.manual_matching(config_dirs, best_config, proc_type, in_args)

            man_matched = pd.read_csv(
                config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
                usecols=['priv_name', 'priv_address', 'Org_ID', 'org_name', 'pub_address','Manual_Match'])

            if in_args.convert_training:
                # Ensure not in recycle mode for training file to be converted
                assert not in_args.recycle, "Failed as convert flag to be used for name_only. Run without --recycle flag."

                conv_file = pd.read_csv(config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
                    usecols=['priv_name_adj', 'priv_address', 'pub_name_adj', 'pub_address', 'Manual_Match'])

                # Convert manual matches file to training json file for use in --recycle (next proc_type i.e. name & address)
                convert_training.convert_to_training(config_dirs, conv_file)

            if in_args.upload_to_db:
                upload_file = pd.read_csv(
                    config_dirs['manual_matches_file'].format(proc_type) + '_' + str(best_config) + '.csv',
                    usecols=['priv_name', 'priv_address', 'Org_ID', 'org_name', 'pub_address', 'Manual_Match'])

                # Add confirmed matches to relevant proc_type table
                if not in_args.recycle:
                    db_calls.add_data_to_table("spaziodati.confirmed_nameonly_matches", config_dirs, proc_type, upload_file)
                if in_args.recycle:
                    db_calls.add_data_to_table("spaziodati.confirmed_nameaddress_matches", config_dirs, proc_type, upload_file)