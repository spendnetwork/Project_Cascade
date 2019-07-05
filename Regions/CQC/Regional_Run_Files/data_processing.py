import pandas as pd
import os
from fuzzywuzzy import fuzz
from tqdm import tqdm
import string
from runfile import Main
import pdb

class DataProcessing(Main):

    def __init__(self, settings):
        super().__init__(settings)

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

    def joinFields(self, df, dftype):
        '''
        Join adjusted name and address strings into single column to be able to calculate levenshtein distance and thereby name and address matches
        :param df: dataframe (registry or source)
        :return: df
        '''

        df[''.join([str(dftype), '_joinfields'])] = df[''.join([str(dftype), '_name_adj'])] + ' ' + df[
            ''.join([str(dftype), '_address_adj'])]
        return df

    def remvStreetNumber(self, df, adj_col):
        df[adj_col] = df[adj_col].str.replace('\d+', '').str.strip()
        return df


class LevDist(Main):
    '''
    Adds the levenshtein distance ratio comparing the amount of change required to convert the source org name
    to the registry org name and therefore a measure of the quality of the match

    :param clust_df: the clustered datafile
    :param output_file: clustered file with added levenshtein distance
    :return: clust_df
    '''

    def __init__(self, settings, clust_df, output_file=None):
        super().__init__(settings)
        self.clust_df = clust_df
        self.output_file = output_file

    def addLevDist(self):
        '''
        Adds the levenshtein distance ratio comparing the amount of change required to convert the source org name
        to the registry org name and therefore a measure of the quality of the match

        :param clust_df: the clustered datafile
        :param output_file: clustered file with added levenshtein distance
        :return: clust_df
        '''
        # Remove company suffixes for more relevant levenshtein distance calculation. Otherwise will have exaggerated
        # Distances if i.e. src name has 'srl' suffix but reg name doesn't.
        self.clust_df['src_name_short'] = self.clust_df.src_name_adj.apply(self.shortenName).astype(str)

        self.clust_df['reg_name_short'] = self.clust_df.reg_name_adj.apply(self.shortenName).astype(str)

        # Add column containing levenshtein distance between the matched registry & source org names
        if 'leven_dist_N' not in self.clust_df.columns:
            self.clust_df['leven_dist_N'], self.clust_df['leven_dist_NA'] = zip(
                *self.clust_df.apply(self.calcMatchRatio, axis=1))

        self.clust_df.to_csv(self.output_file, index=False)

        return self.clust_df

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
            if i in self.org_suffixes.org_suffixes_dict.values():
                rowadj = row.replace(i, '').replace("  ", " ").strip()
        try:
            return str(rowadj)
        except:
            return str(row)

    def calcMatchRatio(self, row):
        """
    	Used in extractMatches() - use fuzzywuzzy to calculate levenshtein distance

    	:return ratio: individual levenshtein distance between the registry and source org string
    	"""
        if pd.notnull(row.src_name_short) and pd.notnull(row.reg_name_short):
            if pd.notnull(row.src_address_adj) and pd.notnull(row.reg_address_adj):
                return int(fuzz.ratio(row.src_name_short, row.reg_name_short)), int(fuzz.ratio(row.src_joinfields, row.reg_joinfields))
            else:
                return int(fuzz.ratio(row.src_name_short, row.reg_name_short)), int(0)


class ProcessSourceData(DataProcessing):
    def __init__(self, settings):
        super().__init__(settings)

    """
	Takes the source data file as input, org type suffixes are replaced with abbreviated versions
	and strings reformatted for consistency across the two datasets

	:return df: the amended source datafile
	"""

    def clean(self):
        raw_data = self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_src_data'].format(
            self.in_args.src_raw_name)
        adj_data = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj_name)

        if not os.path.exists(adj_data):
            df = pd.read_csv(raw_data, usecols=self.raw_src_data_cols,
                             dtype=self.df_dtypes)

            print("Re-organising source data...")
            # Remove punctuation and double spacing in name
            adj_col = str('src_name_adj')
            orig_col = str('src_name')
            df = self.remvPunct(df, orig_col, adj_col)

            # Replace organisation suffixes with standardised version
            df[adj_col].replace(self.org_suffixes.org_suffixes_dict, regex=True, inplace=True)

            # # # Remove punctuation and double spacing in address
            # adj_col = str('src_address_adj')
            # df[adj_col] = df['src_address_streetaddress'] + ', ' + df['src_address_locality'] + ', ' + df['src_address_postalcode'] + ', ' + df['src_address_countryname']
            # df = self.remvPunct(df, adj_col, adj_col)
            # df = self.joinFields(df, 'src')
            # df = df.drop(['src_address_streetaddress', 'src_address_locality', 'src_address_postalcode',
            #               'src_address_countryname'], axis=1)
            print("...done")
            df.to_csv(adj_data, index=False)
        else:
            # Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
            df = pd.read_csv(adj_data, dtype=self.df_dtypes)
        return df


class ProcessRegistryData(DataProcessing):
    """
	Takes the raw registry data file and splits into chunks.
	Multiple address columns are merged into one column,
	org type suffixes are replaced with abbreviated versions and strings reformatted for consistency across the two datasets

	:return dffullmerge: the registry dataframe adjusted as above
	"""

    def clean(self):

        raw_data = self.directories['raw_dir'].format(self.region_dir) + self.directories['raw_reg_data'].format(
            self.in_args.reg_raw_name)
        adj_data = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
            self.in_args.reg_adj_name)

        if not os.path.exists(adj_data):
            print("Re-organising registry data...")
            df = pd.read_csv(raw_data,
                             dtype=self.df_dtypes,
                             chunksize=500000)

            dffullmerge = pd.DataFrame([])
            for chunk in df:

                # Remove punctuation and double spacing
                adj_col = str('reg_name_adj')
                orig_col = str('reg_name')
                chunk = self.remvPunct(chunk, orig_col, adj_col)

                # Replace organisation suffixes with standardised version
                chunk[adj_col].replace(self.org_suffixes.org_suffixes_dict, regex=True, inplace=True)

                # # Remove punctuation and double spacing in address
                # adj_col = str('reg_address_adj')
                # orig_col = str('reg_address')
                #
                # # dfmerge = self.remvPunct(dfmerge, orig_col, adj_col)
                # dfmerge = self.remvPunct(chunk, orig_col, adj_col)

                # dfmerge = self.remvStreetNumber(dfmerge, adj_col)
                # dfmerge = self.joinFields(dfmerge, 'reg')

                # dffullmerge = pd.concat([dffullmerge, dfmerge], ignore_index=True)
                dffullmerge = pd.concat([dffullmerge, chunk], ignore_index=True)

            dffullmerge.drop_duplicates(inplace=True)
            print("...done")

            # dffullmerge['reg_joinfields'] = dffullmerge['reg_joinfields'].astype(str)
            # dffullmerge['reg_source'] = dffullmerge['reg_source'].astype(str)
            # dffullmerge['reg_created_at'] = dffullmerge['reg_created_at'].astype(str)
            # dffullmerge['reg_scheme'] = dffullmerge['reg_scheme'].astype(str)
            # dffullmerge['reg_id'] = dffullmerge['reg_id'].astype(str)
            # dffullmerge['reg_address_adj'] = dffullmerge['reg_address_adj'].astype(str)
            dffullmerge.to_csv(adj_data, index=False)
            return dffullmerge
        else:
            return pd.read_csv(adj_data, dtype=self.df_dtypes)


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
            # If the row is unmatched (has no registry id):
            if pd.isnull(row.reg_id):
                group.at[index, 'reg_id'] = group['reg_id'][max_conf_idx]
                group.at[index, 'reg_name_adj'] = group['reg_name_adj'][max_conf_idx]
                group.at[index, 'reg_name'] = group['reg_name'][max_conf_idx]
                # group.at[index, 'reg_address'] = group['reg_address'][max_conf_idx]
                # group.at[index, 'reg_address_adj'] = group['reg_address_adj'][max_conf_idx]

        return group