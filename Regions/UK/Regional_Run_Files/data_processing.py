import pandas as pd
import os
from fuzzywuzzy import fuzz
from tqdm import tqdm
from Regions.UK.Regional_Run_Files import org_suffixes
import string
import pdb
import numpy as np

def clean_private_data(regiondir, directories, in_args):
    """
	Takes the private data file as input, org type suffixes are replaced with abbreviated versions
	and strings reformatted for consistency across the two datasets

	:return df: the amended private datafile
	"""
    raw_data = directories['raw_dir'].format(regiondir) + directories['raw_priv_data'].format(in_args.priv_raw_name)
    adj_data = directories['adj_dir'].format(regiondir) + directories['adj_priv_data'].format(in_args.priv_adj_name)

    if not os.path.exists(adj_data):
        df = pd.read_csv(raw_data, dtype={'about_or_contact_text': np.str,'home_page_text': np.str})
        print("Re-organising private data...")

        # Remove punctuation and double spacing in name
        adj_col = str('priv_name_adj')
        orig_col = str('priv_name')
        df = remvPunct(df, orig_col, adj_col)

        # Replace organisation suffixes with standardised version

        df[adj_col].replace(org_suffixes.org_suffixes_dict, regex=True, inplace=True)
        df['priv_name_short'] = df.priv_name_adj.apply(shorten_name)

        print("...done")
        df.to_csv(adj_data, index=False)
    else:
        # Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
        df = pd.read_csv(adj_data)
    return df


def clean_matched_data(directories, regiondir, proc_type):
    df = pd.read_csv(directories['match_output_file'].format(regiondir, proc_type))
    adj_col = str('CH_name_adj')
    orig_col = str('CH_name')
    df = remvPunct(df, orig_col, adj_col)

    # Replace organisation suffixes with standardised version
    df[adj_col].replace(org_suffixes.org_suffixes_dict, regex=True, inplace=True)
    df['CH_name_short'] = df.CH_name_adj.apply(shorten_name)
    df.to_csv(directories['match_output_file'].format(regiondir, proc_type),index=False)


def remvPunct(df, orig_col, adj_col):
    """
    :param df: dataframe
    :param orig_col: the original unmodified organisation data strings
    :param adj_col: the orig_col with removed punctuation and standardised org_suffixes
    :return: adjusted dataframe
    """
    df[adj_col] = df[orig_col].str.translate(
        str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
    return df


def shorten_name(row):
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


def assign_pub_data_to_clusters(df, assigned_file=None):
    """
	Unmatched members of a cluster are assigned the public data of the highest-confidence matched
	row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
	by the levenshtein distance during the match extraction phase.

	:param df: the main clustered and matched dataframe
	:param assigned_file : file-path to save location
	:return altered df
	"""

    df.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
    df.reset_index(drop=True, inplace=True)
    tqdm.pandas()
    print("Assigning close matches within clusters...")
    df = df.groupby(['Cluster ID']).progress_apply(get_max_id)
    df.to_csv(assigned_file, index=False)
    return df


def get_max_id(group):
    """
	Used by assign_pub_data_to_clusters(). Takes one entire cluster,
	finds the row with the best confidence score and applies the public data of that row
	to the rest of the rows in that cluster which don't already have matches

	:param group: all rows belonging to one particular cluster
	:return group: the amended cluster to be updated into the main df
	"""
    max_conf_idx = group['Confidence Score'].idxmax()
    for index, row in group.iterrows():
        # If the row is unmatched (has no public org_id):
        if pd.isnull(row.org_id):
            group.at[index, ''] = group['CH_id'][max_conf_idx]
            group.at[index, ''] = group['CH_name'][max_conf_idx]
            group.at[index, 'CH_address'] = group['CH_address'][max_conf_idx]
    return group


def calc_match_ratio(row):
    """
	Used in extract_matches() - use fuzzywuzzy to calculate levenshtein distance

	:return ratio: individual levenshtein distance between the public and private org string
	"""

    if pd.notnull(row.priv_name_short) and pd.notnull(row.CH_name_short):
        return fuzz.ratio(row.priv_name_short, row.CH_name_short)


def add_lev_dist(clust_df, output_file=None):
    '''
    Adds the levenshtein distance ratio comparing the amount of change required to convert the private org name
    to the public org name and therefore a measure of the quality of the match

    :param clust_df: the clustered datafile
    :param output_file: clustered file with added levenshtein distance
    :return: clust_df
    '''
    # Remove company suffixes for more relevant levenshtein distance calculation. Otherwise will have exaggerated
    # Distances if i.e. priv name has 'srl' suffix but pub name doesn't.

    # Add column containing levenshtein distance between the matched public & private org names
    if 'leven_dist_N' not in clust_df.columns:
        clust_df['leven_dist_N'] = clust_df.apply(calc_match_ratio, axis=1)

    clust_df.to_csv(output_file, index=False)

    return clust_df