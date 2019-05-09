import pandas as pd
import os
from fuzzywuzzy import fuzz
from tqdm import tqdm
from Regions.UK.Regional_Run_Files import org_suffixes
import string
import pdb
import numpy as np
from runfile import Main


class DataProcessing(Main):
    # def __init__(self, settings):
    #     super().__init__(settings)

    # def __init__(self, settings):
    #     self.region_dir = settings.region_dir
    #     self.directories = settings.directories
    #     self.in_args = settings.in_args

    def shortenName(self, row):
        """
    	Removes the company suffixes according to the org_suffixes.org_suffixes_dict. This helps with the extraction phase
    	because it improves the relevance of the levenshtein distances.

    	:param row: each row of the dataframe
    	:return row: shortened string i.e. from "coding ltd" to "coding"
    	"""
        row = str(row).replace('-', ' ').replace("  ", " ").strip()
        rowsplit = str(row).split(" ")
        for i in rowsplit:
            if i in org_suffixes.org_suffixes_dict.values():
                rowadj = row.replace(i, '').replace("  ", " ").strip()
        try:
            return rowadj
        except:
            return row

    def remvPunct(self, df, orig_col, adj_col):
        """
        :param df: dataframe
        :param orig_col: the original unmodified organisation data strings
        :param adj_col: the orig_col with removed punctuation and standardised org_suffixes
        :return: adjusted dataframe
        """
        df[adj_col] = df[orig_col].str.translate(
            str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
        return df


class LevDist:
    '''
    Adds the levenshtein distance ratio comparing the amount of change required to convert the source org name
    to the registry org name and therefore a measure of the quality of the match

    :param clust_df: the clustered datafile
    :param output_file: clustered file with added levenshtein distance
    :return: clust_df
    '''

    def __init__(self, clust_df, output_file=None):
        self.clust_df = clust_df
        self.output_file = output_file

    def addLevDist(self):
        # Remove company suffixes for more relevant levenshtein distance calculation. Otherwise will have exaggerated
        # Distances if i.e. src name has 'srl' suffix but reg name doesn't.

        # Add column containing levenshtein distance between the matched registry & source org names
        if 'leven_dist_N' not in self.clust_df.columns:

            self.clust_df['leven_dist_N'] = self.clust_df.apply(self.calcMatchRatio, axis=1)
            # clust_df['leven_dist_N'] = clust_df['leven_dist_N'].astype('int64')
        self.clust_df.to_csv(self.output_file, index=False)

        return self.clust_df

    def calcMatchRatio(self, row):
        """
        Used in extractMatches() - use fuzzywuzzy to calculate levenshtein distance

        :return ratio: individual levenshtein distance between the registry and source org string
        """

        if pd.notnull(row.src_name_short) and pd.notnull(row.CH_name_short):
            return fuzz.ratio(row.src_name_short, row.CH_name_short)



class AssignRegDataToClusters:
    """
    Unmatched members of a cluster are assigned the registry data of the highest-confidence matched
    row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
    by the levenshtein distance during the match extraction phase.

    :param df: the main clustered and matched dataframe
    :param assigned_file : file-path to save location
    :return altered df
    """

    def __init__(self, df, assigned_file=None):
        self.df = df
        self.assigned_file = assigned_file

    def assign(self):
        self.df.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
        self.df.reset_index(drop=True, inplace=True)
        tqdm.pandas()
        print("Assigning close matches within clusters...")
        self.df = self.df.groupby(['Cluster ID']).progress_apply(AssignRegDataToClusters.getMaxId)
        self.df.to_csv(self.assigned_file, index=False)
        return self.df

    def getMaxId(group):
        """
        Used by assign_reg_data_to_clusters(). Takes one entire cluster,
        finds the row with the best confidence score and applies the registry data of that row
        to the rest of the rows in that cluster which don't already have matches

        :param group: all rows belonging to one particular cluster
        :return group: the amended cluster to be updated into the main df
        """
        max_conf_idx = group['Confidence Score'].idxmax()
        for index, row in group.iterrows():
            # If the row is unmatched (has no registry reg_id):
            if pd.isnull(row.reg_id):
                group.at[index, ''] = group['CH_id'][max_conf_idx]
                group.at[index, ''] = group['CH_name'][max_conf_idx]
                group.at[index, 'CH_address'] = group['CH_address'][max_conf_idx]
        return group


class ProcessSourceData(DataProcessing):
    """
    Takes the source data file as input, org type suffixes are replaced with abbreviated versions
    and strings reformatted for consistency across the two datasets

    :return df: the amended source datafile
    """


    def clean(self):

        raw_data = self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_src_data'].format(self.in_args.src_raw_name)
        adj_data = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(self.in_args.src_adj_name)

        if not os.path.exists(adj_data):
            df = pd.read_csv(raw_data, dtype=self.df_dtypes)
            print("Re-organising source data...")

            # Remove punctuation and double spacing in name
            adj_col = str('src_name_adj')
            orig_col = str('src_name')
            df = self.remvPunct(df, orig_col, adj_col)

            # Replace organisation suffixes with standardised version
            df[adj_col].replace(org_suffixes.org_suffixes_dict, regex=True, inplace=True)
            df['src_name_short'] = df.src_name_adj.apply(self.shortenName)

            print("...done")
            df.to_csv(adj_data, index=False)
        else:
            # Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
            df = pd.read_csv(adj_data, dtype=self.df_dtypes)

        return df


class ProcessRegistryData(DataProcessing):

    # def __init__(self, proc_type):
    #     # To add proc_type to the init, must also rebuild the parent init as it will be overridden with nothing otherwise:
    #     Main.__init__(self, self.settings)
    #     self.proc_type = proc_type

    def clean(self):
        df = pd.read_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type))
        adj_col = str('CH_name_adj')
        orig_col = str('CH_name')
        df = self.remvPunct(df, orig_col, adj_col)
        # Replace organisation suffixes with standardised version
        df[adj_col].replace(org_suffixes.org_suffixes_dict, regex=True, inplace=True)
        df['CH_name_short'] = df.CH_name_adj.apply(self.shortenName)
        df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type),index=False)