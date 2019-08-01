import pandas as pd
import os
import subprocess
import numpy as np
from shutil import copyfile
import pdb
from runfile import Main, logging


class Matching(Main):
    def __init__(self, settings, src_df, reg_df):
        super().__init__(settings)
        self.src_df = src_df
        self.reg_df = reg_df

    def dedupe(self):
        # Run dedupe for matching and clustering
        if not os.path.exists(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)):
            if self.in_args.region == 'Italy':

                src_file = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
                    self.in_args.src_adj_name)

                reg_df = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
                    self.in_args.reg_adj_name)

                self.dedupeMatchCluster(src_file, reg_df)

        if not os.path.exists(self.directories['assigned_output_file'].format(self.region_dir, self.proc_type)):

            clust_df = pd.read_csv(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type), index_col=None,
                                   dtype=self.df_dtypes)

        #     # Copy registry data to high-confidence cluster records
            clust_df = self.runfile_mods.data_processing.AssignRegDataToClusters(clust_df, self.directories[
                'assigned_output_file'].format(self.region_dir, self.proc_type)).assign()

            # Adds leven_dist column and verify matches based on config process criteria:
            clust_df = self.runfile_mods.data_processing.LevDist(clust_df,
                                                                 self.directories["assigned_output_file"].format(
                                                                     self.region_dir,
                                                                     self.proc_type)).addLevDist()
        else:
            clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type),
                                   index_col=None, dtype=self.df_dtypes, usecols=self.df_dtypes.keys())


        return clust_df

    def dedupeMatchCluster(self, src_file, reg_df):
        """
    	Deduping - first the registry and source data are matched using dedupes csvlink,
    	then the matched file is put into clusters
        :param reg_df:
        :param src_file:
    	:param directories: file/folder locations
    	:param  config_files: the main config files
    	:param proc_type: the 'type' of the process (Name, Name & Address)
    	:param proc_num: the individual process within the config file
    	:return None
    	:output : matched output file
    	:output : matched and clustered output file
    	"""

        src_fields = self.configs['processes'][self.proc_type][self.proc_num]['dedupe_field_names']['source_data']
        reg_fields = self.configs['processes'][self.proc_type][self.proc_num]['dedupe_field_names']['registry_data']

        train = ['--skip_training' if self.in_args.training else '']
        # Matching:
        if not os.path.exists(self.directories['match_output_file'].format(self.region_dir, self.proc_type)):
            if self.in_args.recycle:

                # Copy manual matching file over to build on for clustering
                copyfile(self.directories['manual_matching_train_backup'].format(self.region_dir),
                         self.directories['manual_training_file'].format(self.region_dir, self.proc_type))

            # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
            if os.path.exists('./learned_settings'):
                os.remove('./learned_settings')

            if os.path.exists('./csvdedupe/csvdedupe/learned_settings'):
                os.remove('./csvdedupe/csvdedupe/learned_settings')


            logging.info("Starting matching...")

            cmd = ['csvlink '
                   + str(src_file) + ' '
                   + str(reg_df)
                   + ' --field_names_1 ' + ' '.join(src_fields)
                   + ' --field_names_2 ' + ' '.join(reg_fields)
                   + ' --training_file ' + self.directories['manual_training_file'].format(self.region_dir, self.proc_type)
                   + ' --output_file ' + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
                   + str(train[0])
                   ]

            p = subprocess.Popen(cmd, shell=True)

            p.wait()
            df = pd.read_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type),
                             usecols=self.settings.dedupe_cols,
                             dtype=self.settings.df_dtypes)
            df = df[pd.notnull(df['src_name'])]
            df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)

        # Clustering:
        if not os.path.exists(self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)):
            # Copy training file from first clustering session if recycle mode
            if self.in_args.recycle:
                copyfile(self.directories['cluster_training_backup'].format(self.region_dir),
                         self.directories['cluster_training_file'].format(self.region_dir, self.proc_type))

            logging.info("Starting clustering...")
            cmd = ['python csvdedupe.py '
                   + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
                   + ' --field_names ' + ' '.join(src_fields) + ' '
                   + str(train[0])
                   + ' --training_file ' + self.directories['cluster_training_file'].format(self.region_dir, self.proc_type)
                   + ' --output_file ' + self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)]
            p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
            p.wait()  # wait for subprocess to finish

            if not self.in_args.recycle:
                # Copy training file to backup, so it can be found and copied into recycle phase clustering
                copyfile(self.directories['cluster_training_file'].format(self.region_dir, self.proc_type),
                         self.directories['cluster_training_backup'].format(self.region_dir))
        else:
            pass


class CascadeExtraction(Main):

    def extract(self, clustdf):
        """
        Import config file containing variable assignments for i.e. char length, match ratio
        Based on the 'cascading' config details, verify matches to new csv

        :return extracts_file: contains dataframe with possible acceptable matches
        """

        levendist = str('leven_dist_N')

        # Round confidence scores to 2dp :

        clustdf['Confidence Score'] = clustdf['Confidence Score'].map(lambda x: round(x, 2))

        # Filter by current match_score:
        clustdf = clustdf[clustdf[levendist] >= self.configs['processes'][self.proc_type][self.proc_num]['min_match_score']]


        # if the earliest process, accept current clustdf as matches, if not (>min):
        if self.proc_num > min(self.configs['processes'][self.proc_type]):
            # If at last proc num, filter for only > min char length to capture remaining long strings
            if self.proc_num == max(self.configs['processes'][self.proc_type]):
                clustdf = clustdf[
                    clustdf.src_name_short.str.len() > self.configs['processes'][self.proc_type][self.proc_num][
                        'char_counts']]
                clustdf = clustdf[clustdf[levendist] <= 99]
            else:
                try:
                    # Filter by char count and previous count (if exists):
                    clustdf = clustdf[
                        clustdf.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num][
                            'char_counts']]
                    clustdf = clustdf[
                        clustdf.src_name_short.str.len() > self.configs['processes'][self.proc_type][self.proc_num - 1][
                            'char_counts']]
                    # Filter by < 99 as first self.proc_num includes all lengths leading to duplicates
                    clustdf = clustdf[clustdf[levendist] <= 99]
                except:
                    clustdf = clustdf[
                        clustdf.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num][
                            'char_counts']]
        else:
            if os.path.exists(self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(
                    self.conf_file_num) + '.csv'):
                # Clear any previous extraction file for this config:
                os.remove(self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(
                    self.conf_file_num) + '.csv')

        # Add process number to column for calculating stats purposes:
        clustdf['process_num'] = str(self.proc_num)

        if not os.path.exists(
                self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv'):
            clustdf.to_csv(
                self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index=False)
            return clustdf
        else:
            extracts_file = pd.read_csv(
                self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index_col=None)
            extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
            extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
            extracts_file.to_csv(
                self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index=False)
            return extracts_file


class VerificationAndUploads(Main):

    def __init__(self, settings, stat_file):
        super().__init__(settings)
        self.stat_file = stat_file


    def verify(self):

        main_proc = self.configs['processes'][self.proc_type][min(self.configs['processes'][self.proc_type].keys())]

        # If recycle arg matches the recycle variable in the self.proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if self.in_args.recycle == main_proc['recycle_phase']:

            # If user has used --config_review flag, set best_config variable based on manual review of stats file...
            if self.in_args.config_review:
                self.best_config = input(
                    (
                        "\nReview Outputs/{0}/Filtered_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
                        self.proc_type))

            else:
                # ...otherwise pick best config_file based on stats file (max leven dist avg):
                max_lev = self.stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
                self.best_config = self.stat_file.at[max_lev, 'Config_File']


            self.manualMatching()

            if self.in_args.convert_training:
                # Ensure not in recycle mode for training file to be converted
                assert not self.in_args.recycle, "Failed as convert flag to be used for name_only. Run excluding --recycle flag."

                conv_file = pd.read_csv(
                    self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
                    usecols=self.training_cols)

                try :
                    # Convert manual matches file to training json file for use in --recycle (next self.proc_type i.e. name & address)
                    self.runfile_mods.convert_training.ConvertToTraining(self, conv_file).convert()
                except AttributeError:
                    # An AttributeError will be raised if the region does not require/have a convert_training module
                    next

            if self.in_args.upload:
                # Add confirmed matches to relevant table
                self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

    def manualMatching(self):
        """
    	Provides user-input functionality for manual matching based on the extracted records
    	:return manual_match_file: extracted file with added column (Y/N/Unsure)
    	"""

        manual_match_file = pd.read_csv(
            self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
            index_col=None)
        manual_match_file['Manual_Match_N'] = ''

        # Automatically confirm rows with leven dist of 100
        for index, row in manual_match_file.iterrows():
            if row.leven_dist_N == 100:
                manual_match_file.at[index, 'Manual_Match_N'] = str('Y')

        if self.in_args.terminal_matching:
            # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
            for index, row in manual_match_file.sample(frac=1).iterrows():
                logging.info(f"\nsource name: {str(row.src_name_adj)}")
                logging.info(f"\nRegistry name: {str(row.reg_name_adj)}")
                logging.info(f"\nLevenshtein distance: {str(row.leven_dist_N)}")
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

            logging.info("Saving...")
            manual_match_file.to_csv(
                self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
                index=False, columns=self.manual_matches_cols)
            # return manual_match_file


        else:

            manual_match_file.to_csv(
                self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
                index=False, columns=self.manual_matches_cols)

            # return manual_match_file

        if not self.in_args.upload:
            logging.info(
                "\nIf required, please perform manual matching process in {} and then run 'python runfile.py --convert_training --upload".format(
                    self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(
                        self.best_config) + '.csv'))
#
# def extractionAndUploads(configs, proc_type, in_args,stat_file, region_dir, directories, settings, convert_training, db_calls):
#
#     main_proc = configs['processes'][proc_type][min(configs['processes'][proc_type].keys())]
#
#     # If recycle arg matches the recycle variable in the proc_type config file (want to restrict operations to just
#     # i.e. Name_Only but still have the dynamics to iterate through proc_types
#     if in_args.recycle == main_proc['recycle_phase']:
#
#         # If user has used --config_review flag, set best_config variable based on manual review of stats file...
#         if in_args.config_review:
#             best_config = input(
#                 (
#                     "\nReview Outputs/{0}/Filtered_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
#                     proc_type))
#         else:
#             # ...otherwise pick best config_file based on stats file (max leven dist avg):
#             max_lev = stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
#             best_config = stat_file.at[max_lev, 'Config_File']
#
#         manualMatching(region_dir, directories, best_config, proc_type, in_args)
#
#         if in_args.convert_training:
#             # Ensure not in recycle mode for training file to be converted
#             assert not in_args.recycle, "Failed as convert flag to be used for name_only. Run excluding --recycle flag."
#
#             conv_file = pd.read_csv(
#                 directories['unverified_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv',
#                 usecols=settings.training_cols)
#
#             # Convert manual matches file to training json file for use in --recycle (next proc_type i.e. name & address)
#             convert_training.convertToTraining(region_dir, directories, conv_file)
#
#         if in_args.upload:
#             # Add confirmed matches to relevant table
#             db_calls.addDataToTable(region_dir, main_proc['db_table'], directories, proc_type, in_args, settings)


def dedupe_matchTEST(src_file, reg_df, region_dir, directories, config_files, proc_type, proc_num, in_args):
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
        logging.info("Starting matching...")

        cmd = ['csvlink '
               + str(src_file) + ' '
               + str(reg_df)
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

#
# def extractMatches(region_dir, clustdf, config_files, directories, proc_num, proc_type, conf_file_num, in_args):
#     """
# 	Import config file containing variable assignments for i.e. char length, match ratio
# 	Based on the 'cascading' config details, verify matches to new csv
#
# 	:return extracts_file: contains dataframe with possible acceptable matches
# 	"""
#
#     if in_args.recycle:
#         levendist = str('leven_dist_NA')
#     else:
#         levendist = str('leven_dist_N')
#
#
#     # Round confidence scores to 2dp :
#     # pdb.set_trace()
#     clustdf['Confidence Score'] = clustdf['Confidence Score'].map(lambda x: round(x, 2))
#
#     # Filter by current match_score:
#     clustdf = clustdf[clustdf[levendist] >= config_files['processes'][proc_type][proc_num]['min_match_score']]
#
#     # if the earliest process, accept current clustdf as matches, if not (>min):
#     if proc_num > min(config_files['processes'][proc_type]):
#         try:
#             # Filter by char count and previous count (if exists):
#             clustdf = clustdf[
#                 clustdf.src_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
#             clustdf = clustdf[
#                 clustdf.src_name_short.str.len() > config_files['processes'][proc_type][proc_num - 1]['char_counts']]
#             # Filter by < 99 as first proc_num includes all lengths leading to duplicates
#             clustdf = clustdf[clustdf[levendist] <= 99]
#         except:
#             clustdf = clustdf[
#                 clustdf.src_name_short.str.len() <= config_files['processes'][proc_type][proc_num]['char_counts']]
#     else:
#         if os.path.exists(directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv'):
#             # Clear any previous extraction file for this config:
#             os.remove(directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv')
#
#     # Add process number to column for calculating stats purposes:
#     clustdf['process_num'] = str(proc_num)
#
#     if not os.path.exists(directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv'):
#         clustdf.to_csv(directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv',
#                        index=False)
#         return clustdf
#     else:
#         extracts_file = pd.read_csv(
#             directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv', index_col=None)
#         extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
#         extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
#         extracts_file.to_csv(directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(conf_file_num) + '.csv',
#                              index=False)
#         return extracts_file
#
#
# def manualMatching(region_dir, directories, best_config, proc_type, in_args):
#     """
# 	Provides user-input functionality for manual matching based on the extracted records
# 	:return manual_match_file: extracted file with added column (Y/N/Unsure)
# 	"""
#
#     manual_match_file = pd.read_csv(
#         directories['filtered_matches'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv', index_col=None)
#     manual_match_file['Manual_Match_N'] = ''
#     manual_match_file['Manual_Match_NA'] = ''
#
#     # Automatically confirm rows with leven dist of 100
#     for index, row in manual_match_file.iterrows():
#         if row.leven_dist_N == 100:
#             manual_match_file.at[index, 'Manual_Match_N'] = str('Y')
#         if row.leven_dist_NA == 100:
#             manual_match_file.at[index, 'Manual_Match_NA'] = str('Y')
#
#     if in_args.terminal_matching:
#         choices = ['n', 'na']
#         choice = input("\nMatching name only or name and address? (N / NA):")
#         while choice.lower() not in choices:
#             choice = input("\nMatching name only or name and address? (N / NA):")
#
#         # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
#         for index, row in manual_match_file.sample(frac=1).iterrows():
#             if choice.lower() == 'n':
#                 logging.info("\nsource name: " + str(row.src_name_adj))
#                 logging.info("\nRegistry name: " + str(row.reg_name_adj))
#                 logging.info("\nLevenshtein distance: " + str(row.leven_dist_N))
#             else:
#                 logging.info("\nsource name: " + str(row.src_name_adj))
#                 logging.info("source address: " + str(row.src_address_adj))
#                 logging.info("\nRegistry name: " + str(row.reg_name_adj))
#                 logging.info("Registry address: " + str(row.reg_address_adj))
#                 logging.info("\nLevenshtein distance : " + str(row.leven_dist_NA))
#
#             match_options = ["y", "n", "u", "f"]
#             match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
#             while match.lower() not in match_options:
#                 match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
#
#             if str(match).lower() != "f":
#                 manual_match_file.at[index, 'Manual_Match_N'] = str(match).capitalize()
#                 # Need to add in NA version ? Might just remove terminal matching altogether...
#                 continue
#             else:
#                 break
#
#         manual_match_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
#
#         logging.info("Saving...")
#         manual_match_file.to_csv(directories['unverified_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv',
#                                  index=False,
#                                  # columns=['reg_id', 'id', 'reg_name',
#                                  #          'src_name','reg_address', 'src_address', 'leven_dist_N', 'leven_dist_NA','Manual_Match_N','Manual_Match_NA'])
#                                 columns = ['Cluster ID', 'leven_dist_N', 'leven_dist_NA', 'reg_id', 'id', 'reg_name', 'reg_name_adj',
#                                            'reg_address', 'src_name', 'src_name_adj', 'src_address', 'src_address_adj', 'reg_address_adj',
#                                            'Manual_Match_N', 'Manual_Match_NA', 'srcjoinfields', 'regjoinfields'])
#         return manual_match_file
#
#     else:
#         manual_match_file.to_csv(directories['unverified_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv',
#                                  index=False,
#                                  # columns=['reg_id', 'id', 'reg_name',
#                                  #          'src_name', 'reg_address', 'src_address', 'leven_dist_N', 'leven_dist_NA',
#                                  #          'Manual_Match_N', 'Manual_Match_NA'])
#                                  columns=['Cluster ID', 'leven_dist_N', 'leven_dist_NA', 'reg_id', 'id', 'reg_name', 'reg_name_adj',
#                                           'reg_address','src_name', 'src_name_adj', 'src_address', 'src_address_adj', 'reg_address_adj', 'Manual_Match_N','Manual_Match_NA', 'srcjoinfields', 'regjoinfields'])
#         if not in_args.recycle:
#             if not in_args.upload:
#                 logging.info("\nIf required, please perform manual matching process in {} and then run 'python runfile.py --convert_training --upload".format(
#                 directories['unverified_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv'))
#         else:
#             if not in_args.upload:
#                 logging.info("\nIf required, please perform manual matching process in {} and then run 'python runfile.py --recycle --upload".format(
#                     directories['unverified_matches_file'].format(region_dir, proc_type) + '_' + str(best_config) + '.csv'))


# def matching(configs, proc_type, df_dtypes, proc_num, directories, in_args, region_dir, runfile_mods):
#     # Run dedupe for matching and calculate related stats for comparison
#     if not os.path.exists(directories["cluster_output_file"].format(region_dir, proc_type)):
#         if in_args.region == 'Italy':
#             src_file = directories['adj_dir'].format(region_dir) + directories['adj_src_data'].format(
#                 in_args.src_adj_name)
#             reg_df = directories['adj_dir'].format(region_dir) + directories['adj_reg_data'].format(
#                 in_args.reg_adj_name)
#
#             runfile_mods.data_matching.dedupe_match_cluster(src_file, reg_df, region_dir, directories, configs,
#                                                            proc_type, proc_num, in_args)
#
#     if not os.path.exists(directories['assigned_output_file'].format(region_dir, proc_type)):
#         clust_df = pd.read_csv(directories["cluster_output_file"].format(region_dir, proc_type), index_col=None,
#                                dtype=df_dtypes)
#
#         # Copy registry data to high-confidence cluster records
#         clust_df = runfile_mods.data_processing.assign_reg_data_to_clusters(clust_df, directories[
#             'assigned_output_file'].format(region_dir, proc_type))
#
#         # Adds leven_dist column and verify matches based on config process criteria:
#         clust_df = runfile_mods.data_processing.add_lev_dist(clust_df,
#                                                             directories["assigned_output_file"].format(region_dir,
#                                                                                                        proc_type))
#
#     else:
#         clust_df = pd.read_csv(directories["assigned_output_file"].format(region_dir, proc_type),
#                                index_col=None, dtype=df_dtypes, usecols=df_dtypes.keys())
#
#     return clust_df


# def dedupeMatchCluster(src_file, reg_df, region_dir, directories, config_files, proc_type, proc_num, in_args):
#     """
# 	Deduping - first the registry and source data are matched using dedupes csvlink,
# 	then the matched file is put into clusters
#     :param reg_df:
#     :param src_file:
# 	:param directories: file/folder locations
# 	:param  config_files: the main config files
# 	:param proc_type: the 'type' of the process (Name, Name & Address)
# 	:param proc_num: the individual process within the config file
# 	:return None
# 	:output : matched output file
# 	:output : matched and clustered output file
# 	"""
#
#     src_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['source_data']
#     reg_fields = config_files['processes'][proc_type][proc_num]['dedupe_field_names']['registry_data']
#
#     train = ['--skip_training' if in_args.training else '']
#     # Matching:
#     if not os.path.exists(directories['match_output_file'].format(region_dir, proc_type)):
#         if in_args.recycle:
#             # Copy manual matching file over to build on for clustering
#             copyfile(directories['manual_matching_train_backup'].format(region_dir), directories['manual_training_file'].format(region_dir, proc_type))
#
#         # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
#         if os.path.exists('./learned_settings'):
#             os.remove('./learned_settings')
#         logging.info("Starting matching...")
#
#         cmd = ['csvlink '
#                + str(src_file) + ' '
#                + str(reg_df)
#                + ' --field_names_1 ' + ' '.join(src_fields)
#                + ' --field_names_2 ' + ' '.join(reg_fields)
#                + ' --training_file ' + directories['manual_training_file'].format(region_dir, proc_type)
#                + ' --output_file ' + directories['match_output_file'].format(region_dir, proc_type) + ' '
#                + str(train[0])
#                ]
#         p = subprocess.Popen(cmd, shell=True)
#
#         p.wait()
#         df = pd.read_csv(directories['match_output_file'].format(region_dir, proc_type),
#                          usecols=['id', 'src_name', 'src_address', 'src_name_adj', 'src_address_adj', 'reg_id', 'reg_name',
#                                   'reg_name_adj', 'reg_address_adj',
#                                   'reg_address', 'reg_address_adj', 'srcjoinfields', 'regjoinfields'],
#                          dtype={'id': np.str, 'src_name': np.str, 'src_address': np.str, 'src_name_adj': np.str, 'src_address_adj': np.str,
#                                 'reg_id': np.str, 'reg_name': np.str, 'reg_name_adj': np.str, 'reg_address': np.str, 'reg_address_adj': np.str, 'srcjoinfields':np.str, 'regjoinfields':np.str})
#         df = df[pd.notnull(df['src_name'])]
#         df.to_csv(directories['match_output_file'].format(region_dir, proc_type), index=False)
#
#     # Clustering:
#     if not os.path.exists(directories['cluster_output_file'].format(region_dir, proc_type)):
#         # Copy training file from first clustering session if recycle mode
#         if in_args.recycle:
#             copyfile(directories['cluster_training_backup'].format(region_dir), directories['cluster_training_file'].format(region_dir, proc_type))
#
#         logging.info("Starting clustering...")
#         cmd = ['python csvdedupe.py '
#                + directories['match_output_file'].format(region_dir, proc_type) + ' '
#                + ' --field_names ' + ' '.join(src_fields) + ' '
#                + str(train[0])
#                + ' --training_file ' + directories['cluster_training_file'].format(region_dir, proc_type)
#                + ' --output_file ' + directories['cluster_output_file'].format(region_dir, proc_type)]
#         p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
#         p.wait()  # wait for subprocess to finish
#
#         if not in_args.recycle:
#             # Copy training file to backup, so it can be found and copied into recycle phase clustering
#             copyfile(directories['cluster_training_file'].format(region_dir, proc_type), directories['cluster_training_backup'].format(region_dir))
#     else:
#         pass

