import pandas as pd
import os
from runfile import Main
from datetime import datetime
import glob
import pdb


class MatchFiltering(Main):
    def __init__(self, settings):
        super().__init__(settings)
        self.filtered_matches = self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' \
                                + str(self.conf_file_num) + '.csv'

    def filter(self, df):
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
        df['Confidence Score'] = df['Confidence Score'].map(lambda x: round(x, 2))

        # Filter by current match_score:
        df = df[df[levendist] >= self.configs['processes'][self.proc_type][self.proc_num]['min_match_score']]

        # If it's not the first process in the config_file...
        if self.proc_num > min(self.configs['processes'][self.proc_type]):
            # If at last proc num...
            if self.proc_num == max(self.configs['processes'][self.proc_type]):
                #  ...filter for only > min char length to capture remaining long strings
                df = df[df.src_name_short.str.len() > self.configs['processes'][self.proc_type][self.proc_num]
                ['char_counts']]
                df = df[df[levendist] <= 99]

            # If it's not first and not the last process...
            else:
                try:
                    # Filter by both char count and previous count (if exists):
                    df = df[df.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num][
                            'char_counts']]
                    df = df[df.src_name_short.str.len() > self.configs['processes'][self.proc_type][self.proc_num - 1]
                    ['char_counts']]
                    # Filter by < 99 as first self.proc_num includes all lengths leading to duplicates
                    df = df[df[levendist] <= 99]
                except:
                    df = df[df.src_name_short.str.len() <= self.configs['processes'][self.proc_type][self.proc_num][
                            'char_counts']]

        # ...else if it is the first process in the dictionary
        else:
            if os.path.exists(self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(
                        self.conf_file_num) + '.csv'):
                # Clear any previous extraction file for this config:
                os.remove(self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + str(
                    self.conf_file_num) + '.csv')

        # Temporarily add process number as column for calculating stats purposes
        df['process_num'] = str(self.proc_num)

        if not os.path.exists(self.filtered_matches):
            df.to_csv(self.filtered_matches, index=False)
        else:
            filtered_file = pd.read_csv(self.filtered_matches, index_col=None, dtype=self.df_dtypes)
            filtered_file = pd.concat([filtered_file, df], ignore_index=True, sort=True)
            filtered_file.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
            filtered_file.to_csv(self.filtered_matches, index=False)
            return filtered_file


class VerificationAndUploads(Main):

    def __init__(self, settings):
        super().__init__(settings)
        self.unverified_file = self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type,
                                                                                  datetime.today().strftime('%Y-%m-%d'))
        self.best_filtered = None

    def verify(self):

        main_proc = self.configs['processes'][self.proc_type][min(self.configs['processes'][self.proc_type].keys())]
        # If recycle arg matches the recycle variable in the self.proc_type config file (want to restrict operations to just
        # i.e. Name_Only but still have the dynamics to iterate through proc_types
        if self.in_args.recycle == main_proc['recycle_phase']:

            # If user has used --config_review flag, set best_config variable based on manual review of stats file...
            if self.in_args.config_review:
                self.best_config = input(
                    (
                    "\nReview Outputs/{0}/Filtered_Matches/Matches_Stats_{0}.csv and choose best config file number:")
                        .format(self.proc_type))
            else:
                # ...otherwise pick best config_file based on stats file (max leven dist avg):
                stat_file = pd.read_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type))
                max_lev = stat_file['Leven_Dist_Avg'].astype('float64').idxmax()
                self.best_config = stat_file.at[max_lev, 'Config_File']

            self.best_filtered = self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + \
                                 str(self.best_config) + '.csv'

            files = glob.glob(
                os.path.join(self.directories['unverified_matches_dir'].format(self.region_dir, self.proc_type), '*'))

            # if there are no files in the unverified folder, then prepare extracted matches file for manual matching:
            if not files:
                self.manualMatching()

            if self.in_args.upload:
                # Add confirmed matches to relevant table
                self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

    def manualMatching(self):
        """
        Provides user-input functionality for manual matching based on the extracted records
        :return manual_match_file: extracted file with added column (Y/N/Unsure)
        """

        best_filtered = pd.read_csv(self.best_filtered, index_col=None, dtype=self.df_dtypes)
        best_filtered['Manual_Match_N'] = ''
        best_filtered['Manual_Match_NA'] = ''

        if self.in_args.terminal_matching:
            # Iterate over the file, shuffled with sample, as best matches otherwise would show first:
            for index, row in best_filtered.sample(frac=1).iterrows():
                print("\nsource name: " + str(row.src_name_adj))
                print("\nRegistry name: " + str(row.reg_name_adj))
                print("\nLevenshtein distance: " + str(row.leven_dist_N))
                match_options = ["y", "n", "u", "f"]
                match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
                while match.lower() not in match_options:
                    match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")

                if str(match).lower() != "f":
                    best_filtered.at[index, 'Manual_Match_N'] = str(match).capitalize()
                    continue
                else:
                    break

            best_filtered.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)

            print("Saving...")
            best_filtered.to_csv(self.unverified_file, index=False, columns=self.dbUpload_cols)
        else:
            best_filtered.to_csv(self.unverified_file, index=False, columns=self.dbUpload_cols)
