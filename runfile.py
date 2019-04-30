import argparse
import pandas as pd
import os
import numpy as np
import ast

from Regions.Italy.Regional_Run_Files.db_calls import checkDataExists
from core_run_files import setup, data_matching, db_calls, convert_training, data_processing, data_analysis
from pathlib import Path
import directories
import pdb

def get_input_args(rootdir, args=None):
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    parser = argparse.ArgumentParser(conflict_handler='resolve') # conflict_handler allows overriding of args (for pytest purposes : see conftest.py::in_args())
    parser.add_argument('--region', default='UK', type=str, help='Define the region/country (Italy/UK)')
    parser.add_argument('--src_raw_name', default='source_data_mini.csv', type=str,
                        help='Set raw source/source datafile name')
    parser.add_argument('--reg_raw_name', default='registry_data.csv', type=str, help='Set raw registry datafile name')
    parser.add_argument('--src_adj_name', default='src_data_adj.csv', type=str,
                        help='Set cleaned source/source datafile name')
    parser.add_argument('--reg_adj_name', default='reg_data_adj.csv', type=str, help='Set cleaned registry datafile name')
    parser.add_argument('--recycle', action='store_true', help='Recycle the manual training data')
    parser.add_argument('--training', action='store_false', help='Modify/contribute to the training data')
    parser.add_argument('--config_review', action='store_true', help='Manually review/choose best config file results')
    parser.add_argument('--terminal_matching', action='store_true', help='Perform manual matching in terminal')
    parser.add_argument('--convert_training', action='store_true', help='Convert confirmed matches to training file for recycle phase')
    parser.add_argument('--upload_to_db', action='store_true' , help='Add confirmed matches to database')

    # Added args as a parameter per https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    args = parser.parse_args(args)

    # If the clustering training file does not exist (therefore the matching train file too as this is created before the former)
    # Force an error and prompt user to add the training flag
    if args.training == True and not os.path.exists(os.path.join(rootdir,args.region,"Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json")):
        print("Dedupe training files do not exist - running with --training flag to initiate training process")
        parser.add_argument('--training', action='store_true', help='Modify/contribute to the training data')

    args = parser.parse_args()

    return args, parser


def main(regiondir, in_args, directories):

    setup.setupRawDirs(regiondir, directories)
    # Ignores directories - convention is <num>_config.py
    pyfiles = "*_config.py"

    try:
        # For each config file read it and convert to dictionary for accessing
        for conf_file in config_path.glob(pyfiles):
            with open(conf_file) as config_file:
                file_contents = []
                file_contents.append(config_file.read())

                # Convert list to dictionary
                configs = ast.literal_eval(file_contents[0])

                conf_file_num = int(conf_file.name[0])

                # Clean registry and source datasets for linking
                # source df needed in memory for stats
                srcdf = data_processing.clean_source_data(regiondir, directories, in_args)
                if not in_args.recycle:
                    data_processing.clean_registry_data(regiondir, directories, in_args)

                # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                for proc_type in configs['processes']:
                    if in_args.recycle == configs['processes'][proc_type][min(configs['processes'][proc_type].keys())]['recycle_phase']:

                        # Create working directories if don't exist
                        setup.setup_dirs(directories, regiondir, proc_type)

                        # Iterate over each process number in the config file
                        for proc_num in configs['processes'][proc_type]:
                            # # Get first process from config file
                            # main_proc = min(configs['processes'][proc_type].keys())

                            # Define data types for clustered file. Enables faster loading.
                            clustdtype = {'Cluster ID': np.float64, 'Confidence Score': np.float,
                                          'id': np.float, 'src_name': np.str, 'src_address': np.str, 'src_address_adj': np.str,
                                          'src_name_adj': np.str, 'reg_id': np.str, 'reg_name_adj': np.str,
                                          'reg_address': np.str, 'reg_address_adj': np.str,'src_name_short': np.str, 'reg_name_short': np.str,
                                          'leven_dist_N': np.int,'leven_dist_NA': np.int, 'reg_name': np.str, 'srcjoinfields': np.str, 'regjoinfields':np.str}

                            # Run dedupe for matching and calculate related stats for comparison
                            if not os.path.exists(directories["cluster_output_file"].format(regiondir, proc_type)):
                                src_file = directories['adj_dir'].format(regiondir) + directories['adj_src_data'].format(in_args.src_adj_name)
                                reg_file = directories['adj_dir'].format(regiondir) + directories['adj_reg_data'].format(in_args.reg_adj_name)

                                data_matching.dedupe_match_cluster(src_file, reg_file, regiondir, directories, configs, proc_type, proc_num, in_args)

                            if not os.path.exists(directories['assigned_output_file'].format(regiondir, proc_type)):
                                clust_df = pd.read_csv(directories["cluster_output_file"].format(regiondir, proc_type),index_col=None, dtype=clustdtype)
                                # Copy registry data to high-confidence cluster records
                                # clust_df = data_processing.assign_reg_data_to_clusters(clust_df, directories[
                                #     'assigned_output_file'].format(regiondir, proc_type))
                                clust_df=clust_df[:20]
                                # Adds leven_dist column and extract matches based on config process criteria:
                                clust_df = data_processing.add_lev_dist(clust_df, directories["assigned_output_file"].format(regiondir, proc_type))

                            else:

                                clust_df = pd.read_csv(directories["assigned_output_file"].format(regiondir, proc_type),
                                                       index_col=None, dtype=clustdtype, usecols=clustdtype.keys())

                            extracts_file = data_matching.extract_matches(regiondir, clust_df, configs, directories, proc_num,
                                                                          proc_type,
                                                                          conf_file_num, in_args)
                        break
                    else:
                        continue
                # Output stats file:
                stat_file = data_analysis.calc_matching_stats(regiondir, clust_df, extracts_file, directories, conf_file_num,
                                                              proc_type, srcdf, in_args)

    except StopIteration:
        # Continue if no more config files found

        print("Done")

        # For each process type (eg: Name & Add, Name only) outlined in the configs file:
    for proc_type in configs['processes']:

        # If recycle arg matches the recycle variable in the proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if in_args.recycle == configs['processes'][proc_type][min(configs['processes'][proc_type].keys())][
            'recycle_phase']:

            # If user has used --config_review flag, set best_config variable based on manual review of stats file...
            if in_args.config_review:
                best_config = input(
                    (
                        "\nReview Outputs/{0}/Extracted_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
                        proc_type))
            else:
                # ...otherwise pick best config_file based on stats file (max leven dist avg):
                max_lev = stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
                best_config = stat_file.at[max_lev, 'Config_File']

            data_matching.manual_matching(regiondir, directories, best_config, proc_type, in_args)

            if in_args.convert_training:
                # Ensure not in recycle mode for training file to be converted
                assert not in_args.recycle, "Failed as convert flag to be used for name_only. Run excluding --recycle flag."

                conv_file = pd.read_csv(
                    directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
                    usecols=['src_name_adj', 'src_address_adj', 'reg_name_adj', 'reg_address_adj', 'Manual_Match_N','Manual_Match_NA'])

                # Convert manual matches file to training json file for use in --recycle (next proc_type i.e. name & address)
                convert_training.convert_to_training(regiondir, directories, conv_file)

            if in_args.upload_to_db:
                upload_file = pd.read_csv(
                    directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
                    usecols=['src_name', 'src_address', 'reg_id', 'reg_name', 'reg_address', 'Manual_Match_N','Manual_Match_NA'])

                # Add confirmed matches to relevant proc_type table
                if not in_args.recycle:
                    db_calls.add_data_to_table(regiondir, "spaziodati.confirmed_nameonly_matches", directories, proc_type,
                                               upload_file, in_args)
                    print("Process complete. Run 'python runfile.py --recycle' to begin training against additional fields.")
                if in_args.recycle:
                    db_calls.add_data_to_table(regiondir, "spaziodati.confirmed_nameaddress_matches", directories, proc_type,
                                               upload_file, in_args)

if __name__ == '__main__':
    rootdir = os.path.dirname(os.path.abspath(__file__))
    in_args, _ = get_input_args(rootdir)
    regiondir = os.path.join(rootdir, 'Regions', in_args.region)
    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None
    # Define config file variables and related arguments
    config_path = Path(os.path.join(regiondir, 'Config_Files'))
    directories = directories.dirs["dirs"]


    # Ignores directories - convention is <num>_config.py
    pyfiles = "*_config.py"

    if not in_args.recycle:
        # If registry/registry data file doesn't exist, pull from database
        checkDataExists(regiondir, directories, in_args, "spaziodati.sd_sample")

    main(regiondir, in_args, directories)
