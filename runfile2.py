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
    parser.add_argument('--src_raw_name', default='source_data.csv', type=str,
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
    if args.training == True and not os.path.exists(os.path.join(rootdir,"Regions",args.region,"Data_Inputs/Training_Files/Name_Only/Clustering/cluster_training.json")):
        print("Dedupe training files do not exist - running with --training flag to initiate training process")
        parser.add_argument('--training', action='store_true', help='Modify/contribute to the training data')

    args = parser.parse_args()

    return args, parser


# def main(region_dir, in_args, directories, config_path, settings):
#
#     runfile_mods = settings.runfile_mods
#     data_processing = runfile_mods.data_processing
#     data_analysis = runfile_mods.data_analysis
#     db_calls = runfile_mods.db_calls
#     setup = runfile_mods.setup
#     data_matching = runfile_mods.data_matching
#
#     if not in_args.recycle:
#         try:
#             # If registry/registry data file doesn't exist, pull from database
#             # db_calls.checkDataExists(region_dir, directories, in_args, settings.registryTableSource)
#             # FIX THIS OR USE ABOVE
#             db_calls.checkDataExists(settings)
#         except:
#             # Will fail if checkDataExists function doesn't exist (i.e. registry data sourced externally (not from db))
#             pass
#
#     setup.setupRawDirs(region_dir, directories)
#
#     try:
#         # For each config file read it and convert to dictionary for accessing
#         pyfiles = "*_config.py"
#         for conf_file in config_path.glob(pyfiles):
#             with open(conf_file) as config_file:
#                 file_contents = []
#                 file_contents.append(config_file.read())
#
#                 # Convert list to dictionary
#                 configs = ast.literal_eval(file_contents[0])
#                 settings.configs = configs
#
#                 conf_file_num = int(conf_file.name[0])
#                 settings.conf_file_num = conf_file_num
#
#                 # Clean registry and source datasets for linking
#                 # source df needed in memory for stats
#                 # src_df = data_processing.ProcessSourceData(region_dir, directories, in_args).clean()
#                 src_df = data_processing.ProcessSourceData(settings).clean()
#
#                 if not in_args.recycle:
#                     try:
#                         # data_processing.ProcessRegistryData(region_dir, directories, in_args).clean()
#                         data_processing.ProcessRegistryData(settings).clean()
#                     except AttributeError:
#                         # Skip if registry data not downloaded yet (i.e. UK)
#                         next
#
#                 # For each process type (eg: Name & Add, Name only) outlined in the configs file:
#                 for proc_type in configs['processes']:
#                     settings.proc_type = proc_type
#
#                     # # Get first process from config file
#                     main_proc = min(configs['processes'][proc_type].keys())
#                     settings.upload_table = main_proc['db_table']
#
#                     # If args.recycle matches the recycle setting for the first process type
#                     if in_args.recycle == configs['processes'][proc_type][main_proc]['recycle_phase']:
#
#                         # Create working directories if don't exist
#                         setup.Setup(settings).setupDirs()
#
#                         # Iterate over each process number in the config file
#                         for proc_num in configs['processes'][proc_type]:
#                             settings.proc_num = proc_num
#
#                             # # Define data types for clustered file. Enables faster loading.
#                             # df_dtypes = settings.df_dtypes
#
#                             # Run dedupe for matching and calculate related stats for comparison
#                             if in_args.region == 'Italy':
#                                 # clust_df = data_matching.matching(configs, settings, df_dtypes, proc_num, directories, in_args, region_dir, runfile_mods)
#                                 clust_df = data_matching.Matching(settings)
#
#                             if in_args.region == 'UK':
#                                 clust_df = data_matching.Matching(settings).dedupe(src_df)
#
#                             extracts_file = data_matching.extractMatches(settings, clust_df)
#                         break
#                     else:
#                         continue
#                 # Output stats file:
#                 stat_file = data_analysis.StatsCalculations(settings, clust_df, extracts_file, src_df).calculate()
#
#     except StopIteration:
#         # Continue if no more config files found
#         print("Done")
#
#         # For each process type (eg: Name & Add, Name only) outlined in the configs file:
#     for proc_type in configs['processes']:
#         settings.proc_type = proc_type
#         # data_matching.ExtractionAndUploads(configs, proc_type, in_args,stat_file, data_matching, region_dir, directories, settings, runfile_mods, db_calls)
#         data_matching.ExtractionAndUploads(settings, stat_file).extract()

class Main:
    def __init__(self, settings):
        self.directories = settings.directories
        self.in_args = settings.in_args
        self.proc_type = settings.proc_type
        self.region_dir = settings.region_dir
        # self.configs = settings.configs
        self.df_dtypes = settings.df_dtypes
        # self.proc_num = settings.proc_num
        self.training_cols = settings.training_cols
        self.manual_matches_cols = settings.manual_matches_cols
        # self.main_proc = settings.main_proc
        self.config_path = settings.config_path
        # Runfile mods
        self.runfile_mods = settings.runfile_mods
        self.data_processing = self.runfile_mods.data_processing
        self.data_analysis = self.runfile_mods.data_analysis
        self.db_calls = self.runfile_mods.db_calls
        self.setup = self.runfile_mods.setup
        self.data_matching = self.runfile_mods.data_matching

    def run_main(self):

        if not in_args.recycle:
            try:
                # If registry/registry data file doesn't exist, pull from database
                self.db_calls.checkDataExists(self.region_dir, self.directories, self.in_args, settings.registryTableSource)
                # FIX THIS OR USE ABOVE
                # self.db_calls.checkDataExists(settings)
            except:
                # Will fail if checkDataExists function doesn't exist (i.e. registry data sourced externally (not from db))
                pass

        self.setup.Setup(settings).setupRawDirs()

        try:
            # For each config file read it and convert to dictionary for accessing
            pyfiles = "*_config.py"
            for conf_file in self.config_path.glob(pyfiles):
                with open(conf_file) as config_file:
                    file_contents = []
                    file_contents.append(config_file.read())

                    # Convert list to dictionary
                    configs = ast.literal_eval(file_contents[0])
                    settings.configs = configs

                    conf_file_num = int(conf_file.name[0])
                    settings.conf_file_num = conf_file_num

                    # Clean registry and source datasets for linking
                    # source df needed in memory for stats
                    # src_df = data_processing.ProcessSourceData(region_dir, directories, in_args).clean()
                    src_df = self.data_processing.ProcessSourceData(settings).clean()

                    if not in_args.recycle:
                        try:
                            # data_processing.ProcessRegistryData(region_dir, directories, in_args).clean()
                            self.data_processing.ProcessRegistryData(settings).clean()
                        except AttributeError:
                            # Skip if registry data not downloaded yet (i.e. UK)
                            next

                    # For each process type (eg: Name & Add, Name only) outlined in the configs file:
                    for proc_type in configs['processes']:
                        settings.proc_type = proc_type

                        # # Get first process from config file
                        main_proc = min(configs['processes'][proc_type].keys())
                        pdb.set_trace()
                        settings.upload_table = str(main_proc['db_table'])

                        # If args.recycle matches the recycle setting for the first process type
                        if in_args.recycle == configs['processes'][proc_type][main_proc]['recycle_phase']:

                            # Create working directories if don't exist
                            self.setup.Setup(settings).setupDirs()

                            # Iterate over each process number in the config file
                            for proc_num in configs['processes'][proc_type]:
                                settings.proc_num = proc_num

                                # # Define data types for clustered file. Enables faster loading.
                                # df_dtypes = settings.df_dtypes

                                # Run dedupe for matching and calculate related stats for comparison
                                if in_args.region == 'Italy':
                                    # clust_df = data_matching.matching(configs, settings, df_dtypes, proc_num, directories, in_args, region_dir, runfile_mods)
                                    clust_df = self.data_matching.Matching(settings)

                                if in_args.region == 'UK':
                                    clust_df = self.data_matching.Matching(settings).dedupe(src_df)

                                extracts_file = self.data_matching.extractMatches(settings, clust_df)
                            break
                        else:
                            continue
                    # Output stats file:
                    stat_file = self.data_analysis.StatsCalculations(settings, clust_df, extracts_file, src_df).calculate()

        except StopIteration:
            # Continue if no more config files found
            print("Done")

            # For each process type (eg: Name & Add, Name only) outlined in the configs file:
        for proc_type in configs['processes']:
            settings.proc_type = proc_type
            # data_matching.ExtractionAndUploads(configs, proc_type, in_args,stat_file, data_matching, region_dir, directories, settings, runfile_mods, db_calls)
            self.data_matching.ExtractionAndUploads(settings, stat_file).extract()


if __name__ == '__main__':

    rootdir = os.path.dirname(os.path.abspath(__file__))
    in_args, _ = getInputArgs(rootdir)

    # Silence warning for df['process_num'] = str(proc_num)
    pd.options.mode.chained_assignment = None

    if in_args.region == 'Italy':
        settings = settings.Italy_Settings

    if in_args.region == 'UK':
        settings = settings.UK_Settings

    settings.in_args = in_args
    settings.region_dir = os.path.join(rootdir, 'Regions', in_args.region)
    # pdb.set_trace()
    # Define config file variables and related data types file
    settings.config_path = Path(os.path.join(settings.region_dir, 'Config_Files'))

    Main(settings).run_main()

