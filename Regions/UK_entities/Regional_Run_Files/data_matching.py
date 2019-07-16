import pandas as pd
import os
import subprocess
import numpy as np
from shutil import copyfile
import pdb
from runfile import Main
from datetime import datetime
from csvdedupe.csvlink import launch_new_instance as launch_matching
from csvdedupe.csvdedupe import launch_new_instance as launch_clustering
import sys

class Matching(Main):
    def __init__(self, settings, src_df, reg_df):
        super().__init__(settings)
        self.src_df = src_df
        self.reg_df = reg_df

    def dedupe(self):

        # Run dedupe for matching and clustering
        if not os.path.exists(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)):

            src_file = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
                self.in_args.src_adj_name)

            reg_df = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
                self.in_args.reg_adj_name)

            self.dedupeMatchCluster(src_file, reg_df)

        if not os.path.exists(self.directories['assigned_output_file'].format(self.region_dir, self.proc_type)):

            clust_df = pd.read_csv(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type), index_col=None,
                                   dtype=self.df_dtypes)

        #     # Copy registry data to high-confidence cluster records
            clust_df = self.data_processing.AssignRegDataToClusters(clust_df, self.directories[
                'assigned_output_file'].format(self.region_dir, self.proc_type)).assign()

            clust_df = clust_df.fillna(value="")

            # Adds leven_dist column and verify matches based on config process criteria:
            clust_df = self.data_processing.LevDist(self, clust_df, self.directories["assigned_output_file"].format(
                                                                     self.region_dir,
                                                                     self.proc_type)).addLevDist()
            clust_df = clust_df[pd.notnull(clust_df['src_name'])]
        else:
            # clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type),index_col=None, dtype=self.df_dtypes)
            clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type),
                                   dtype=self.df_dtypes)
            clust_df = clust_df[pd.notnull(clust_df['src_name'])]

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
            # DO NOT UNCOMMENT - CAUSES BUG
            # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
            # if os.path.exists('./learned_settings'):
            #     os.remove('./learned_settings')
            #
            # if os.path.exists('./csvdedupe/csvdedupe/learned_settings'):
            #     os.remove('./csvdedupe/csvdedupe/learned_settings')

            print("Starting matching...")

            # cmd = ['csvlink '
            #        + str(src_file) + ' '
            #        + str(reg_df)
            #        + ' --field_names_1 ' + ' '.join(src_fields)
            #        + ' --field_names_2 ' + ' '.join(reg_fields)
            #        + ' --training_file ' + self.directories['manual_training_file'].format(self.region_dir, self.proc_type)
            #        + ' --output_file ' + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
            #        + str(train[0])
            #        ]
            #
            # p = subprocess.Popen(cmd, shell=True)
            # p.wait()
            #
            # sys.argv = ['/Users/davidmellor/Code/Spend_Network/Data_Projects/csvdedupe/csvdedupe/csvlink.py',
            #             'csvlink'
            #             '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Data_Inputs/Adj_Data/src_data_adj.csv',
            #             '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Data_Inputs/Adj_Data/reg_data_adj.csv',
            #             '--field_names_1', 'src_name_adj', '--field_names_2', 'reg_name_adj', '--training_file',
            #             '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Data_Inputs/Training_Files/Name_Only/Matching/matching_training.json',
            #             '--output_file',
            #             '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Outputs/Name_Only/Deduped_Data/Name_Only_matched.csv',
            #             '--skip_training']
            sys.argv = [
                        'csvlink',
                        str(src_file),
                        str(reg_df),
                        '--field_names_1', ' '.join(src_fields), '--field_names_2', ' '.join(reg_fields), '--training_file',
                        self.directories['manual_training_file'].format(self.region_dir, self.proc_type),
                        '--sample_size', '500',
                        '--settings_file',os.path.join(os.getcwd(), 'learned_settings'),
                        '--output_file',
                        self.directories['match_output_file'].format(self.region_dir, self.proc_type),
                        str(train[0])
            ]

            launch_matching()

            df = pd.read_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type),
                             usecols=self.dedupe_cols,
                             dtype=self.df_dtypes)
            df = df[pd.notnull(df['src_name'])]
            df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)

        # Clustering:
        if not os.path.exists(self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)):
            # Copy training file from first clustering session if recycle mode
            if self.in_args.recycle:
                copyfile(self.directories['cluster_training_backup'].format(self.region_dir),
                         self.directories['cluster_training_file'].format(self.region_dir, self.proc_type))

            print("Starting clustering...")

            # cmd = ['python csvdedupe.py '
            #        + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
            #        + ' --field_names ' + ' '.join(src_fields) + ' '
            #        + str(train[0])
            #        + ' --training_file ' + self.directories['cluster_training_file'].format(self.region_dir, self.proc_type)
            #        + ' --output_file ' + self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)]

            # cmd = ['csvdedupe '
            #        + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
            #        + ' --field_names ' + ' '.join(src_fields) + ' '
            #        + str(train[0])
            #        + ' --training_file ' + self.directories['cluster_training_file'].format(self.region_dir,
            #                                                                                 self.proc_type)
            #        + ' --output_file ' + self.directories['cluster_output_file'].format(self.region_dir,
            #                                                                             self.proc_type)]

            # p = subprocess.Popen(cmd, cwd=os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
            # p = subprocess.Popen(cmd, shell=True)
            # p.wait()  # wait for subprocess to finish

            sys.argv = [
                        'csvdedupe',
                        self.directories['match_output_file'].format(self.region_dir, self.proc_type),
                        '--field_names',
                        ' '.join(src_fields),
                        str(train[0]),
                        '--training_file',
                        self.directories['cluster_training_file'].format(self.region_dir, self.proc_type),
                        '--settings_file', os.path.join(os.getcwd(), 'learned_settings'),
                        '--output_file',
                        self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)
                        ]

            launch_clustering()

            if not self.in_args.recycle:
                # Copy training file to backup, so it can be found and copied into recycle phase clustering
                copyfile(self.directories['cluster_training_file'].format(self.region_dir, self.proc_type),
                         self.directories['cluster_training_backup'].format(self.region_dir))
        else:
            pass


class CascadeExtraction(Main):
    def __init__(self, settings):
        super().__init__(settings)

    def extract(self, clustdf):
        """
        Import config file containing variable assignments for i.e. char length, match ratio
        Based on the 'cascading' config details, verify matches to new csv

        :return extracts_file: contains dataframe with possible acceptable matches
        """
        if self.in_args.recycle:
            levendist = str('leven_dist_NA')
        else:
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
                        clustdf.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num]['char_counts']]
                    clustdf = clustdf[
                        clustdf.src_name_short.str.len() > self.configs['processes'][self.proc_type][self.proc_num - 1][
                            'char_counts']]
                    # Filter by < 99 as first self.proc_num includes all lengths leading to duplicates
                    clustdf = clustdf[clustdf[levendist] <= 99]
                except:
                    clustdf = clustdf[
                        clustdf.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num]['char_counts']]
        else:
            if os.path.exists(self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv'):
                # Clear any previous extraction file for this config:
                os.remove(self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(
                    self.conf_file_num) + '.csv')

        # Add process number to column for calculating stats purposes:
        clustdf['process_num'] = str(self.proc_num)

        if not os.path.exists(
                self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv'):
            clustdf.to_csv(
                self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index=False)
            # return clustdf
        else:
            extracts_file = pd.read_csv(
                self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index_col=None, dtype=self.df_dtypes)
            extracts_file = pd.concat([extracts_file, clustdf], ignore_index=True, sort=True)
            extracts_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
            extracts_file.to_csv(
                self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.conf_file_num) + '.csv',
                index=False)
            return extracts_file


class VerificationAndUploads(Main):

    def __init__(self, settings):
        super().__init__(settings)

    def verify(self):

        main_proc = self.configs['processes'][self.proc_type][min(self.configs['processes'][self.proc_type].keys())]
        # If recycle arg matches the recycle variable in the self.proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if self.in_args.recycle == main_proc['recycle_phase']:

            # If user has used --config_review flag, set best_config variable based on manual review of stats file...
            if self.in_args.config_review:
                self.best_config = input(
                    (
                        "\nReview Outputs/{0}/Extracted_Matches/Matches_Stats_{0}.csv and choose best config file number:").format(
                        self.proc_type))

            else:
                # ...otherwise pick best config_file based on stats file (max leven dist avg):
                stat_file = pd.read_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type))
                max_lev = stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
                self.best_config = stat_file.at[max_lev, 'Config_File']

            self.manualMatching()

            if self.in_args.upload:
                # Add confirmed matches to relevant table
                self.runfile_mods.db_calls.DbCalls(self).addDataToTable()


    def manualMatching(self):
        """
    	Provides user-input functionality for manual matching based on the extracted records
    	:return manual_match_file: extracted file with added column (Y/N/Unsure)
    	"""

        manual_match_file = pd.read_csv(
            self.directories['extract_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv',
            index_col=None, dtype=self.df_dtypes)
        manual_match_file['Manual_Match_N'] = ''
        manual_match_file['Manual_Match_NA'] = ''

        # Automatically confirm rows with leven dist of 100
        for index, row in manual_match_file.iterrows():
            if row.leven_dist_N == 100:
                manual_match_file.at[index, 'Manual_Match_N'] = str('Y')
            if row.leven_dist_NA == 100:
                manual_match_file.at[index, 'Manual_Match_NA'] = str('Y')

        if self.in_args.terminal_matching:
            # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
            for index, row in manual_match_file.sample(frac=1).iterrows():
                print("\nsource name: " + str(row.src_name_adj))
                print("\nRegistry name: " + str(row.reg_name_adj))
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
            manual_match_file.to_csv(
                self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type, datetime.today().strftime('%Y-%m-%d')),
                # index=False, columns=self.manual_matches_cols)
                index=False, columns=self.dbUpload_cols)
        else:
            manual_match_file.to_csv(
                self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type, datetime.today().strftime('%Y-%m-%d')),
                # index=False, columns=self.manual_matches_cols)
                index=False, columns=self.dbUpload_cols)
