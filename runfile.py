import argparse
import pandas as pd
import os
import ast
from pathlib import Path
import yaml
import settings
import datetime
import logging.config
import sys
import pdb

from core.logging_config import add_papertrail_logging_to_webapps, config_stdout_root_logger_with_papertrail


def getInputArgs(rootdir, args=None):
    """
	Assign arguments including defaults to pass to the python call

	:return: arguments variable for both directory and the data file
	"""
    parser = argparse.ArgumentParser(conflict_handler='resolve') # conflict_handler allows overriding of args (for pytest purposes : see conftest.py::in_args())
    parser.add_argument('--region', default='UK_entities', type=str, help='Define the region/country (Italy/UK)')
    parser.add_argument('--src', default='source_data.csv', type=str,
                        help='Set raw source/source datafile name')
    parser.add_argument('--reg', default='registry_data.csv', type=str, help='Set raw registry datafile name')
    parser.add_argument('--src_adj', default='src_data_adj.csv', type=str,
                        help='Set cleaned source/source datafile name')
    parser.add_argument('--reg_adj', default='reg_data_adj.csv', type=str, help='Set cleaned registry datafile name')
    parser.add_argument('--recycle', action='store_true', help='Recycle the manual training data')
    parser.add_argument('--training', action='store_false', help='Modify/contribute to the training data')
    parser.add_argument('--config_review', action='store_true', help='Manually review/choose best config file results')
    parser.add_argument('--terminal_matching', action='store_true', help='Perform manual matching in terminal')
    parser.add_argument('--convert_training', action='store_true', help='Convert confirmed matches to training file for recycle phase')
    parser.add_argument('--upload', action='store_true' , help='Add confirmed matches to database')
    parser.add_argument('--clear_all', action='store_true', help='Clear all datafiles')
    parser.add_argument('--clear_adj', action='store_true', help='Clear all files except raw data')
    parser.add_argument('--clear_outputs', action='store_true', help='Clear all files except inputs')
    parser.add_argument('--clear_post_matching', action='store_true', help='Clear all files after matching phase')
    parser.add_argument('--data_date', type=str,
                        default=(datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                        , help='Set source data date to be downloaded from (inclusive)')
    parser.add_argument('--prodn_unverified', action='store_true', help='DO NOT USE - for production scripts only to transfer matches to s3 buckets ')
    parser.add_argument('--prodn_verified', action='store_true', help='DO NOT USE - for production scripts only to transfer matches to s3 buckets ')
    parser.add_argument('--split', action='store_true', help='split source file and initiate segmented matching')

    # Added args as a parameter per https://stackoverflow.com/questions/55259371/pytest-testing-parser-error-unrecognised-arguments/55260580#55260580
    args = parser.parse_args(args)

    # If the clustering training file does not exist (therefore the matching train file too as this is created before the former)
    # Force an error and prompt user to add the training flag
    if args.training == True and not os.path.exists(os.path.join(rootdir,"Regions",args.region,"Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json")):
        print("Dedupe training files do not exist - running with --training flag to initiate training process")
        parser.add_argument('--training', action='store_true', help='Modify/contribute to the training data')

    args = parser.parse_args()

    return args, parser


class Main:
    def __init__(self, settings):
        # Defined in __main__
        self.directories = settings.directories
        self.in_args = settings.in_args
        self.region_dir = settings.region_dir
        self.config_path = settings.config_path
        self.settings = settings

        # Defined in settings file
        self.df_dtypes = settings.df_dtypes
        self.stats_cols = settings.stats_cols
        self.training_cols = settings.training_cols
        self.manual_matches_cols = settings.manual_matches_cols
        self.dbUpload_cols = settings.dbUpload_cols
        self.proc_type = settings.proc_type
        self.dedupe_cols = settings.dedupe_cols
        self.reg_data_source = settings.reg_data_source
        self.src_data_source = settings.src_data_source
        self.raw_src_data_cols = settings.raw_src_data_cols

        # Runfile modules
        self.runfile_mods = settings.runfile_mods
        self.data_processing = self.runfile_mods.data_processing
        self.data_analysis = self.runfile_mods.data_analysis
        self.db_calls = self.runfile_mods.db_calls
        self.setup = self.runfile_mods.setup
        self.data_matching = self.runfile_mods.data_matching
        self.convert_training = self.runfile_mods.convert_training
        self.org_suffixes = self.runfile_mods.org_suffixes
        self.match_filtering = self.runfile_mods.match_filtering

        # Defined during runtime
        self.main_proc = settings.main_proc
        self.configs = settings.configs
        self.conf_file_num = settings.conf_file_num
        self.proc_num = settings.proc_num
        self.upload_table = settings.upload_table
        self.transfer_table = settings.transfer_table
        self.best_config = settings.best_config

    def run_main(self):

        # Build project folders
        self.setup.Setup(self).setupRawDirs()

        # Delete csvs from previous runs
        self.setup.ClearFiles(self).clearFiles()

        # If not using recycle mode
        if not in_args.recycle:
            try:
                # If registry/registry data file doesn't exist, pull from database
                self.db_calls.FetchData.checkDataExists(self)
            except:
                # Will fail if checkDataExists function doesn't exist (i.e. registry data sourced externally (not from db))
                pass

        try:
            # For each config file read it and convert to dictionary for accessing
            pyfiles = "*_config.py"
            for conf_file in self.config_path.glob(pyfiles):
                with open(conf_file) as config_file:
                    file_contents = []
                    file_contents.append(config_file.read())

                    # Convert list to dictionary
                    configs = ast.literal_eval(file_contents[0])
                    self.configs = configs

                    conf_file_num = int(conf_file.name[0])
                    self.conf_file_num = conf_file_num

                    # Clean registry and source datasets for linking
                    # source df needed in memory for stats
                    src_df = self.data_processing.ProcessSourceData(self).clean()

                    try:
                        reg_df = self.data_processing.ProcessRegistryData(self).clean()
                    except FileNotFoundError:
                        # Skip if registry data not downloaded yet (i.e. UK)
                        next

                    # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                    for proc_type in configs['processes']:
                        self.proc_type = proc_type

                        # Get first process number from config file
                        main_proc_num = min(configs['processes'][proc_type].keys())
                        main_proc_configs = configs['processes'][proc_type][main_proc_num]

                        self.upload_table = main_proc_configs['upload_table']
                        self.transfer_table = main_proc_configs['transfer_table']

                        # If args.recycle matches the recycle setting for the first process type
                        if in_args.recycle == main_proc_configs['recycle_phase']:

                            # Create working directories if don't exist
                            self.setup.Setup(self).SetupDirs()

                            # Iterate over each process number in the config file
                            for proc_num in configs['processes'][proc_type]:
                                self.proc_num = proc_num

                                # Run dedupe for matching and calculate related stats for comparison
                                if in_args.region == 'UK':
                                    clust_df = self.data_matching.Matching(self, src_df).dedupe()
                                else:
                                    clust_df = self.data_matching.Matching(self, src_df, reg_df).dedupe()

                                filtered_matches = self.match_filtering.MatchFiltering(self).filter(clust_df)
                            break
                        else:
                            continue
                    # Output stats file:
                    self.data_analysis.StatsCalculations(self, clust_df, filtered_matches, src_df).calculate()

        except StopIteration:
            # Continue if no more config files found
            print("Done")

        self.match_filtering.VerificationAndUploads(self).verify()

        if in_args.region == 'UK_entities':
            self.AWS_calls = self.runfile_mods.AWS_calls
            self.AWS_calls.AwsTransfers(self).transfer()


if __name__ == '__main__':

    rootdir = os.path.dirname(os.path.abspath(__file__))
    in_args, _ = getInputArgs(rootdir)

    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None

    if in_args.region == 'Italy':
        settings = settings.Italy_Settings

    if in_args.region == 'UK':
        settings = settings.UK_Settings

    if in_args.region == 'UK_entities':
        settings = settings.UK_entities

    if in_args.region == 'CQC':
        settings = settings.CQC_settings

    # If any production flags are being called use production logger...
    if in_args.prodn_verified or in_args.prodn_unverified:
        config_stdout_root_logger_with_papertrail(app_name='entity_matching', level=logging.DEBUG)

    # ...else use local logger
    else:
        # Import logging configs
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f.read())
            logging.config.dictConfig(config)

        def exception_handler(type, value, tb):
            logging.exception('Uncaught exception: {0}'.format(str(value)))

        # Install exception handler
        sys.excepthook = exception_handler

    settings.in_args = in_args
    settings.region_dir = os.path.join(rootdir, 'Regions', in_args.region)

    # Define config file variables and attach to settings object
    settings.config_path = Path(os.path.join(settings.region_dir, 'Config_Files'))

    Main(settings).run_main()