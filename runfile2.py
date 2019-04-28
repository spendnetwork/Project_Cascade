import argparse
import pandas as pd
import os
import ast
from pathlib import Path
import pdb
import settings

def getInputArgs(rootdir, args=None):
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""

    parser = argparse.ArgumentParser(conflict_handler='resolve') # conflict_handler allows overriding of args (for pytest purposes : see conftest.py::in_args())
    parser.add_argument('--region', default='UK', type=str, help='Define the region/country (Italy/UK)')
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

    # Added args as a parameter per https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    args = parser.parse_args(args)

    # If the clustering training file does not exist (therefore the matching train file too as this is created before the former)
    # Force an error and prompt user to add the training flag
    if args.training == True and not os.path.exists(os.path.join(rootdir,args.region,"Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json")):
        print("Dedupe training files do not exist - running with --training flag to initiate training process")
        parser.add_argument('--training', action='store_true', help='Modify/contribute to the training data')

    args = parser.parse_args()

    return args, parser


def main(regiondir, in_args, directories, config_path, settings, runfilemods):

    data_processing = runfilemods.data_processing
    data_analysis = runfilemods.data_analysis
    db_calls = runfilemods.db_calls
    setup = runfilemods.setup
    data_matching = runfilemods.data_matching
    convert_training = runfilemods.convert_training

    if not in_args.recycle:
        try:
            # If public/registry data file doesn't exist, pull from database
            db_calls.checkDataExists(regiondir, directories, in_args, settings.publicTableSource)
        except:
            # Will fail if checkDataExists function doesn't exist (i.e. public data sourced externally (not from db))
            pass

    setup.setupRawDirs(regiondir, directories)

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

                # Clean public and private datasets for linking
                # private df needed in memory for stats
                privdf = data_processing.cleanPrivateData(regiondir, directories, in_args)
                if not in_args.recycle:
                    try:
                        data_processing.cleanPublicData(regiondir, directories, in_args)
                    except AttributeError:
                        # Skip if function clean_public_data not required for this region
                        next

                # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                for proc_type in configs['processes']:
                    # # Get first process from config file

                    main_proc = min(configs['processes'][proc_type].keys())
                    if in_args.recycle == configs['processes'][proc_type][main_proc]['recycle_phase']:

                        # Create working directories if don't exist
                        setup.setupDirs(directories, regiondir, proc_type)

                        # Iterate over each process number in the config file
                        for proc_num in configs['processes'][proc_type]:

                            # Define data types for clustered file. Enables faster loading.
                            df_dtypes = settings.df_dtypes

                            # Run dedupe for matching and calculate related stats for comparison
                            if in_args.region == 'Italy':
                                clust_df = data_matching.matching(configs, proc_type, df_dtypes, proc_num, directories, in_args, regiondir, runfilemods)

                            if in_args.region == 'UK':
                                clust_df = data_matching.matching(configs, proc_type, directories, regiondir, runfilemods, privdf, in_args, proc_num, df_dtypes)

                            extracts_file = data_matching.extractMatches(regiondir, clust_df, configs, directories, proc_num,
                                                                          proc_type,
                                                                          conf_file_num, in_args)
                        break
                    else:
                        continue
                # Output stats file:
                stat_file = data_analysis.calcMatchingStats(regiondir, clust_df, extracts_file, directories, conf_file_num,
                                                              proc_type, privdf, in_args)

    except StopIteration:
        # Continue if no more config files found

        print("Done")

        # For each process type (eg: Name & Add, Name only) outlined in the configs file:
    for proc_type in configs['processes']:

        main_proc = configs['processes'][proc_type][min(configs['processes'][proc_type].keys())]

        # If recycle arg matches the recycle variable in the proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if in_args.recycle == main_proc['recycle_phase']:

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

            data_matching.manualMatching(settings.regiondir, directories, best_config, proc_type, in_args)

            if in_args.convert_training:
                # Ensure not in recycle mode for training file to be converted
                assert not in_args.recycle, "Failed as convert flag to be used for name_only. Run excluding --recycle flag."

                conv_file = pd.read_csv(
                    directories['manual_matches_file'].format(regiondir, proc_type) + '_' + str(best_config) + '.csv',
                    usecols=settings.training_cols)

                # Convert manual matches file to training json file for use in --recycle (next proc_type i.e. name & address)
                convert_training.convertToTraining(regiondir, directories, conv_file)

            if in_args.upload_to_db:
                # Add confirmed matches to relevant table
                db_calls.addDataToTable(regiondir, main_proc['db_table'], directories, proc_type, in_args, settings)


if __name__ == '__main__':

    rootdir = os.path.dirname(os.path.abspath(__file__))
    in_args, _ = getInputArgs(rootdir)

    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None

    if in_args.region == 'Italy':
        settings = settings.Italy_settings

    if in_args.region == 'UK':
        settings = settings.UK_settings

    settings.in_args = in_args
    settings.regiondir = os.path.join(rootdir, 'Regions', in_args.region)

    # Define config file variables and related data types file
    settings.config_path = Path(os.path.join(settings.regiondir, 'Config_Files'))

    main(settings.regiondir, settings.in_args, settings.directories, settings.config_path, settings, settings.runfilemods)
