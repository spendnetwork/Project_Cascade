import pandas as pd
import os
from fuzzywuzzy import fuzz
from tqdm import tqdm
import numpy as np
import org_suffixes
from Config_Files import config_dirs
import string


def clean_private_data(config_dirs, in_args):
    """
	Takes the private data file as input, org type suffixes are replaced with abbreviated versions
	and strings reformatted for consistency across the two datasets

	:return df: the amended private datafile
	"""
    raw_data = config_dirs['raw_dir'] + config_dirs['raw_priv_data'].format(in_args.priv_raw_name)
    adj_data = config_dirs['adj_dir'] + config_dirs['adj_priv_data'].format(in_args.priv_adj_name)

    if not os.path.exists(adj_data):
        df = pd.read_csv(raw_data, usecols=['id', 'supplier_name', 'supplier_streetadd'],
                         dtype={'supplier_name': np.str, 'supplier_streetadd': np.str})
        df.rename(columns={'supplier_name': 'priv_name', 'supplier_streetadd': 'priv_address'}, inplace=True)
        print("Re-organising private data...")
        # Remove punctuation and double spacing
        df['priv_name_adj'] = df['priv_name'].str.translate(
            str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
        df['priv_name_adj'].replace(org_suffixes.org_type_dict, regex=True, inplace=True)
        print("...done")
        df.to_csv(adj_data, index=False)
    else:
        # Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
        df = pd.read_csv(adj_data, usecols=['id', 'priv_name', 'priv_name_adj', 'priv_address'],
                         dtype={'id': np.str, 'priv_name': np.str, 'priv_address': np.str, 'priv_name_adj': np.str})
    return df


def clean_public_data(config_dirs, in_args):
    """
	Takes the raw public data file and splits into chunks.
	Multiple address columns are merged into one column,
	org type suffixes are replaced with abbreviated versions and strings reformatted for consistency across the two datasets

	:return dffullmerge: the public dataframe adjusted as above
	"""
    raw_data = config_dirs['raw_dir'] + config_dirs['raw_pub_data'].format(in_args.pub_raw_name)
    adj_data = config_dirs['adj_dir'] + config_dirs['adj_pub_data'].format(in_args.pub_adj_name)

    if not os.path.exists(adj_data):
        print("Re-organising public data...")
        df = pd.read_csv(raw_data,
                         usecols={'org_name', 'street_address1', 'street_address2', 'street_address3', 'Org_ID'},
                         dtype={'org_name': np.str, 'street_address1': np.str, 'street_address2': np.str,
                                'street_address3': np.str, 'Org_ID': np.str},
                         chunksize=500000)

        dffullmerge = pd.DataFrame([])
        for chunk in df:
            # Remove punctuation and double spacing
            chunk['pub_name_adj'] = chunk['org_name'].str.translate(
                str.maketrans({key: None for key in string.punctuation})).str.replace("  ", " ").str.lower().str.strip()
            chunk['pub_name_adj'].replace(org_suffixes.org_type_dict, regex=True, inplace=True)
            ls = []
            # Merge multiple address columns into one column
            for idx, row in tqdm(chunk.iterrows()):
                ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row['street_address1']]))
                for key in ['street_address2', 'street_address3']:
                    if pd.notnull(row[key]):
                        ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row[key]]))
            labels = ['Org_ID', 'org_name', 'pub_name_adj', 'street_address']
            dfmerge = pd.DataFrame.from_records(ls, columns=labels)
            dffullmerge = pd.concat([dffullmerge, dfmerge], ignore_index=True)
        dffullmerge.drop_duplicates(inplace=True)
        dffullmerge.rename(columns={'street_address': 'pub_address'}, inplace=True)
        print("...done")

        dffullmerge.to_csv(adj_data, index=False)
    else:
        dffullmerge = pd.read_csv(adj_data, usecols=['Org_ID', 'org_name', 'pub_name_adj', 'pub_address'],
                                  dtype={'Org_ID': np.str, 'org_name': np.str, 'pub_name_adj': np.str,
                                         'pub_address': np.str})
    return dffullmerge

def shorten_name(row):
    """
	Removes the company suffixes according to the org_suffixes.org_type_dict. This helps with the extraction phase
	because it improves the relevance of the levenshtein distances.

	:param row: each row of the dataframe
	:return row: shortened string i.e. from "coding ltd" to "coding"
	"""
    row = str(row).replace('-', ' ').replace("  ", " ").strip()
    rowsplit = str(row).split(" ")
    for i in rowsplit:
        if i in org_suffixes.org_type_dict.values():
            rowadj = row.replace(i, '').strip()
    try:
        return rowadj
    except:
        return row


def assign_pub_data_to_clusters(df, assigned_file):
    """
	Unmatched members of a cluster are assigned the public data of the highest-confidence matched
	row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
	by the levenshtein distance during the match extraction phase.

	:param df: the main clustered and matched dataframe
	:param assigned_file : file-path to save location
	:return altered df
	"""

    st = set(df['Cluster ID'])
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
        if pd.isnull(row.Org_ID):
            group.at[index, 'Org_ID'] = group['Org_ID'][max_conf_idx]
            group.at[index, 'pub_name_adj'] = group['pub_name_adj'][max_conf_idx]
            group.at[index, 'pub_address'] = group['pub_address'][max_conf_idx]
    return group


def calc_match_ratio(row):
    """
	Used in extract_matches() - use fuzzywuzzy to calculate levenshtein distance

	:return ratio: individual levenshtein distance between the public and private org string
	"""
    if pd.notnull(row.priv_name_short) and pd.notnull(row.pub_name_short):
        return fuzz.ratio(row.priv_name_short, row.pub_name_short)

def add_lev_dist(clust_df, clustdtype, proc_type):
    # Remove company suffixes for more relevant levenshtein distance calculation
    clust_df['priv_name_short'] = clust_df.priv_name_adj.apply(shorten_name)

    clust_df['pub_name_short'] = clust_df.pub_name_adj.apply(shorten_name)

    # Add column containing levenshtein distance between the matched public & private org names
    if 'leven_dist' not in clust_df.columns:
        clust_df['leven_dist'] = clust_df.apply(calc_match_ratio, axis=1)

    clust_df.to_csv(config_dirs["assigned_output_file"].format(proc_type), index=False)

    return clust_df