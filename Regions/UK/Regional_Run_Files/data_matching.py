import subprocess
import time
import logging
import chwrapper
import math
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
import pandas as pd
import pdb
from fuzzywuzzy import fuzz
from runfile import Main, logging
# import settings

load_dotenv(find_dotenv())
companieshouse_key = os.environ.get("API_KEY2")
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


class Matching(Main):
    def __init__(self, settings, src_df):
        super().__init__(settings)
        self.src_df = src_df

    def dedupe(self):
        if not os.path.exists(self.directories['match_output_file'].format(self.region_dir, self.proc_type)):
            self.companiesHouseMatching(self.src_df)

        self.runfile_mods.data_processing.ProcessRegistryData(self).clean()

        if not os.path.exists(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)):
            # Run dedupe for matching and calculate related stats for comparison
            self.dedupeCluster()

        if not os.path.exists(self.directories['assigned_output_file'].format(self.region_dir, self.proc_type)):

            clust_df = pd.read_csv(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type), index_col=None)

            clust_df = self.runfile_mods.data_processing.AssignRegDataToClusters(clust_df, self.directories[
                'assigned_output_file'].format(self.region_dir, self.proc_type)).assign()


            # Adds leven_dist column and verify matches based on config process criteria:
            clust_df = self.runfile_mods.data_processing.LevDist(clust_df,
                                                            self.directories["assigned_output_file"].format(self.region_dir,
                                                                                                       self.proc_type)).addLevDist()
        else:
            clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type), index_col=None)
        return clust_df

    def companiesHouseMatching(self, df):
        """
        Lookup company name via Companies House API and return company number
        :param df: pandas dataframe containing the organisation name
        :return df: Amended dataframe containing additional company information
        """

        s = chwrapper.Search(access_token=companieshouse_key)
        # Tried matching to src_name_adj but results were poor. Keeping adj/short column for clustering...
        org_strings = df['src_name'].astype(str)
        ch_name_dict = {}
        ch_id_dict = {}
        ch_addr_dict = {}
        chunk_propn = 0

        # Split org_string array into multiple arrays.
        # api states max batch of 600...
        # array_split doesn't have to have equal batch sizes.
        for chunk in np.array_split(org_strings,
                                    math.ceil(len(org_strings) / 500), axis=0):

            logging.info("\nProcessing companies house batch of size: " + str(len(chunk)))

            # For each org_string in the sub-array of org_strings
            # pull org data from companies house
            for word in tqdm(chunk):
                response = s.search_companies(word)
                if response.status_code == 200:
                    # response.json() returns a nested dict with complete org info
                    dict = response.json()
                    name = ''
                    chId = 0
                    chAddr = ''
                    dict = dict['items']

                    for i in range(len(dict)):
                        if fuzz.ratio(word, dict[i]['title']) > fuzz.ratio(word, name):
                            name = dict[i]['title']
                            chId = dict[i]['company_number']
                            chAddr = dict[i]['address_snippet']
                        else:
                            continue

                    ch_name_dict[word] = name
                    ch_id_dict[name] = chId
                    ch_addr_dict[name] = chAddr
                    ch_name_dict.update(ch_name_dict)
                    ch_id_dict.update(ch_id_dict)
                    ch_addr_dict.update(ch_addr_dict)

                elif response.status_code == 404:
                    logger.debug("Error requesting CH data: %s %s",
                                 response.status_code, response.reason)
                elif response.status_code == 429:
                    logger.debug("Error requesting CH data: %s %s",
                                 response.status_code, response.reason)
                    logger.debug("Waiting...")
                    time.sleep(60)
                    s = chwrapper.Search(access_token=companieshouse_key)
                else:
                    logger.error("Error requesting CH data: %s %s",
                                 response.status_code, response.reason)
            chunk_propn += int(len(chunk))
            logging.info("\nProgress: " + str(chunk_propn) + " of " +
                  str(len(org_strings)))

            df['CH_name'] = df['src_name'].map(ch_name_dict)
            df['CH_id'] = df['CH_name'].map(ch_id_dict)
            df['CH_address'] = df['CH_name'].map(ch_addr_dict)

        # Remove error matches
        df = df.dropna(axis=0, subset=['CH_name'])

        df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)

    def dedupeCluster(self):
        """
    	Deduping - first the registry and source data are matched using dedupes csvlink,
    	then the matched file is put into clusters
        :param reg_file:
        :param 	:param directories: file/folder locations
    	:param  config_files: the main config files
    	:param proc_type: the 'type' of the process (Name, Name & Address)
    	:param proc_num: the individual process within the config file
    	:return None
    	:output : matched output file
    	:output : matched and clustered output file
    	"""

        src_fields = self.configs['processes'][self.proc_type][self.proc_num]['dedupe_field_names']['source_data']

        train = ['--skip_training' if self.in_args.training else '']

        # Remove learned_settings (created from previous runtime) file as causes dedupe to hang sometimes, but isn't required
        if os.path.exists('./learned_settings'):
            os.remove('./learned_settings')

        if os.path.exists('./csvdedupe/csvdedupe/learned_settings'):
            os.remove('./csvdedupe/csvdedupe/learned_settings')

        # Clustering:
        if not os.path.exists(self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)):
            # Copy training file from first clustering session if recycle mode

            logging.info("Starting clustering...")
            cmd = ['python csvdedupe.py '
                   + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
                   + ' --field_names ' + ' '.join(src_fields) + ' '
                   + str(train[0])
                   + ' --training_file ' + self.directories['cluster_training_file'].format(self.region_dir, self.proc_type)
                   + ' --output_file ' + self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)]
            p = subprocess.Popen(cmd, cwd=os.path.join(os.getcwd(),'csvdedupe/csvdedupe'), shell=True)
            p.wait()  # wait for subprocess to finish

        else:
            pass


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
                logging.info("\nsource name: " + str(row.src_name_adj))
                logging.info("\nRegistry name: " + str(row.reg_name_adj))
                logging.info("\nLevenshtein distance: " + str(row.leven_dist_N))
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