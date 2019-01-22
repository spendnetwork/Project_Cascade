import pdb
import argparse
import pandas as pd
import os
import subprocess
import re
from pathlib import Path
import os.path as path
from fuzzywuzzy import fuzz
from tqdm import tqdm
from time import time
import numpy as np
import math
import glob
import ast
from Config_Files import config_dirs
import sys
import json
from collections import defaultdict

global org_type_dict
org_type_dict = {
	'società a responsabilità limitata semplificata': 's.r.l.s.',
    'società\' a responsabilità\' limitata semplificata': 's.r.l.s.',
    'societa a responsabilita limitata semplificata': 's.r.l.s.',
    'societa\' a responsabilita\' limitata semplificata': 's.r.l.s.',
    'società a responsabilità limitata': 's.r.l.',
    'società\' a responsabilità\' limitata': 's.r.l.',
    'societa a responsabilita limitata': 's.r.l.',
    'societa\' a responsabilita\' limitata': 's.r.l.',
    's r l': 's.r.l.',
    ' srl ': ' s.r.l. ',  # srl - added whitespace
    ' srl': ' s.r.l.',  # srl - added whitespace left (for EO string)
    's. r. l.': 's.r.l.',
    's r l s': 's.r.l.s.',
    ' srls': ' s.r.l.s.',  # whitespace left
    ' srls ': ' s.r.l.s. ',   # whitespace
    's. r. l. s.': 's.r.l.s.',
    'società per azioni': 's.p.a.',
    'societa per azioni': 's.p.a.',
    's p a': 's.p.a.',
    's. p. a.': 's.p.a.',
    ' spa ': ' s.p.a. ',  # whitespace
    ' spa': ' s.p.a.',  # whitespace left
    'Società in nome collettivo': 's.n.c.',
    'Societa in nome collettivo': 's.n.c.',
    's n c': 's.n.c.',
    ' snc ': ' s.n.c. ',  # whitespace
    ' snc': ' s.n.c.',  # whitespace left
    's. n. c.': 's.n.c.',
    'società in accomandita semplice': 's.a.s.',
    'societa in accomandita semplice': 's.a.s.',
    's a s': 's.a.s.',
    's. a. s.': 's.a.s.',
    ' sas ': ' s.a.s. ',  # whitespace
    ' sas': ' s.a.s.',  # whitespace left
    'società in accomandita semplice': 's.a.s.',
    'societa in accomandita semplice': 's.a.s.',
    's a s': 's.a.s.',
    's. a. s.': 's.a.s.',
    ' sas ': ' s.a.s. ',  # whitespace
    ' sas': ' s.a.s.',  # whitespace left
    'societa cooperativa sociale' : 's.c.s.',
    'società cooperativa sociale' : 's.c.s.',
    's c s' : 's.c.s.',
    's. c. s.' : 's.c.s.',
    ' scs ' : 's.c.s.',
    ' scs' : 's.c.s.'
				}


def clean_private_data(configs, config_dirs):
	'''
	Takes the private data file, org type suffixes are replaced with abbreviated versions 
	and strings reformatted for consistency across the two datasets

	:return df: the amended private datafile
	'''

	raw_data = os.getcwd() + str(config_dirs['raw_dir']) + str(config_dirs['raw_priv_data'])
	adj_data = os.getcwd() + str(config_dirs['adj_dir']) + str(config_dirs['adj_priv_data'])

	if not os.path.exists(adj_data):
		df = pd.read_csv(raw_data, usecols=['id','supplier_name','supplier_streetadd'], dtype={'supplier_name':np.str, 'supplier_streetadd':np.str})
		df.rename(columns = {'supplier_name':'priv_name', 'supplier_streetadd': 'priv_address'}, inplace = True)
		df['priv_name_adj'] = df['priv_name'].str.lower().str.replace('-',' ').str.strip()
		print("Re-organising private data...")
		tqdm.pandas()
		df['priv_name_adj'].replace(org_type_dict, regex=True, inplace=True)
		print("...done")
		df.to_csv(adj_data, index=False)
	else:
		# Specify usecols and  dtypes to prevent mixed dtypes error and remove 'unnamed' cols:
		df = pd.read_csv(adj_data, usecols=['id','priv_name','priv_name_adj','priv_address'], dtype={'id':np.str ,'priv_name':np.str, 'priv_address':np.str, 'priv_name_adj':np.str})
	return df


def clean_public_data(configs, config_dirs):
	'''
	Takes the raw public data file and splits into chunks. 
	Multiple address columns are merged into one column, 
	org type suffixes are replaced with abbreviated versions and strings reformatted for consistency across the two datasets

	:return dffullmerge: the public dataframe adjusted as above
	'''
	raw_data = os.getcwd() + str(config_dirs['raw_dir']) + str(config_dirs['raw_pub_data'])
	adj_data = os.getcwd() + str(config_dirs['adj_dir']) + str(config_dirs['adj_pub_data'])
	
	if not os.path.exists(adj_data):
		print("Re-organising public data...")
		df = pd.read_csv(raw_data, \
							usecols={'org_name','street_address1','street_address2','street_address3','Org_ID'}, \
							dtype={'org_name':np.str, 'street_address1':np.str,'street_address2':np.str, 'street_address3':np.str, 'Org_ID':np.str}, \
							chunksize=500000)
		
		dffullmerge = pd.DataFrame([])
		for chunk in df:
			chunk['pub_name_adj'] = chunk['org_name'].str.lower().str.replace('-',' ').str.strip()
			chunk['pub_name_adj'].replace(org_type_dict, regex=True, inplace=True)
			ls = []
			for idx, row in tqdm(chunk.iterrows()):
				ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row['street_address1']]))
				for key in ['street_address2', 'street_address3']:
					if pd.notnull(row[key]):
						ls.append(tuple([row['Org_ID'], row['org_name'], row['pub_name_adj'], row[key]]))
			labels = ['Org_ID', 'org_name', 'pub_name_adj', 'street_address']
			dfmerge = pd.DataFrame.from_records(ls, columns=labels)
			dffullmerge = pd.concat([dffullmerge, dfmerge], ignore_index=True)
		dffullmerge.drop_duplicates(inplace=True)
		dffullmerge.rename(columns = {'street_address': 'pub_address'}, inplace = True)
		print("...done")

		dffullmerge.to_csv(adj_data, index=False)
	else:
		dffullmerge = pd.read_csv(adj_data, usecols=['Org_ID', 'org_name','pub_name_adj','pub_address'], dtype={'Org_ID':np.str, 'org_name':np.str, 'pub_name_adj':np.str, 'pub_address':np.str })
	return dffullmerge


def dedupe_match_cluster(dirs,proc_fields):
	'''
	Deduping - first the public and private data are matched using dedupes csvlink, 
	then the matched file is put into clusters
	:param dirs: file/folder locations
	:param proc_fields: the fields used for matching
	:return None
	:output : matched output file
	:output : matched and clustered output file 
	'''
	
	priv_fields = proc_fields['dedupe_field_names']['private_data']
	pub_fields = proc_fields['dedupe_field_names']['public_data']
	priv_file = os.getcwd() + str(dirs['adj_dir'] + dirs['adj_priv_data'])
	pub_file = os.getcwd() + str(dirs['adj_dir'] + dirs['adj_pub_data'])
	# Matching:
	if not os.path.exists(os.getcwd() + str(proc_fields['match_output_file'])):
		
		print("Starting matching...")
		cmd = ['csvlink '
				+ str(priv_file) + ' '
				+ str(pub_file)
				+ ' --field_names_1 ' + ' '.join(priv_fields) \
				+ ' --field_names_2 ' + ' '.join(pub_fields) \
				+ ' --training_file ' + os.getcwd() + str(proc_fields['match_training_file']) \
				+ ' --output_file ' + os.getcwd() + str(proc_fields['match_output_file'])]
		p = subprocess.Popen(cmd, shell=True)
		p.wait() 

		df = pd.read_csv(os.getcwd() + str(proc_fields['match_output_file']), usecols=['id','priv_name','priv_address','priv_name_adj','Org_ID','pub_name_adj','pub_address'], dtype = {'id': np.str,'priv_name': np.str,'priv_address': np.str,'priv_name_adj': np.str,'Org_ID': np.str,'pub_name_adj': np.str,'pub_address': np.str})
		df = df[pd.notnull(df['priv_name'])]
		df.to_csv(os.getcwd() + str(proc_fields['match_output_filt_file']), index=False)

	# Clustering:
	if not os.path.exists(os.getcwd() + str(proc_fields['cluster_output_file'])):
		print("Starting clustering...")
		cmd = ['python csvdedupe.py '
				+ os.getcwd() + str(proc_fields['match_output_filt_file']) + ' '
				+ ' --field_names ' + ' '.join(priv_fields) \
				+ ' --training_file ' + os.getcwd() + str(proc_fields['cluster_training_file']) \
				+ ' --output_file ' + os.getcwd() + str(proc_fields['cluster_output_file'])]
		p = subprocess.Popen(cmd, cwd= os.getcwd() + '/csvdedupe/csvdedupe', shell=True)
		p.wait() # wait for subprocess to finish
	else:	
		pass

def shorten_name(row):
	'''
	Removes the company suffixes according to the org_type_dict. This helps with the extraction phase 
	because it improves the relevance of the levenshtein distances.

	:param row: each row of the dataframe
	:return row: shortened string i.e. from "coding ltd" to "coding"
	'''
	row = str(row).replace('-',' ').replace("  "," ").strip()
	rowsplit = str(row).split(" ")
	for i in rowsplit:
		if i in org_type_dict.values():
			rowadj = row.replace(i, '').strip()
	try:
		return rowadj
	except:
		return row

def assign_pub_data_to_clusters(df, assigned_file):
    '''
    Unmatched members of a cluster are assigned the public data of the highest-confidence matched
    row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
    by the levenshtein distance during the match extraction phase.

    :param df: the main clustered and matched dataframe
    :param assigned_file : file-path to save location
    :return altered df
    '''

    st = set(df['Cluster ID'])
    df.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
    df.reset_index(drop=True, inplace=True)
    tqdm.pandas()
    df = df.groupby(['Cluster ID']).progress_apply(get_max_id)
    df.to_csv(assigned_file, index=False)
    return df
    

def get_max_id(group):
	'''
	Used by assign_pub_data_to_clusters(). Takes one entire cluster, 
	finds the row with the best confidence score and applies the public data of that row 
	to the rest of the rows in that cluster which don't already have matches

	:param group: all rows belonging to one particular cluster
	:return group: the amended cluster to be updated into the main df
	'''
	max_conf_idx = group['Confidence Score'].idxmax()
	for index, row in group.iterrows():
		# If the row is unmatched (has no public org_id):
		if pd.isnull(row.Org_ID):
			group.at[index, 'Org_ID'] =  group['Org_ID'][max_conf_idx]
			group.at[index, 'pub_name_adj'] =  group['pub_name_adj'][max_conf_idx]
			group.at[index, 'pub_address'] =  group['pub_address'][max_conf_idx]
	return group


def calc_match_ratio(row):
	'''
	Used in extract_matches() - use fuzzywuzzy to calculate levenshtein distance

	:return ratio: individual levenshtein distance between the public and private org string
	'''
	if pd.notnull(row.priv_name_short) and pd.notnull(row.pub_name_short):
		return fuzz.ratio(row.priv_name_short, row.pub_name_short)


def extract_matches(df, proc_fields, dirs, proc_num, proc_type, processes, conf_file_num):
	'''
	Import config file containing variable assignments for i.e. char length, match ratio
	Based on the 'cascading' config details, extract matches to new csv

	:return extracts_file: contains dataframe with possible acceptable matches
	'''

	# Add column containing levenshtein distance between the matched public & private org names
	if 'leven_dist' not in df.columns:
		df['leven_dist'] = df.apply(calc_match_ratio, axis=1)

	# Filter by current match_score:
	df = df[df['leven_dist'] >= proc_fields['min_match_score']]
	# if the earliest process, accept current df as matches, if not (>min): 
	if proc_num > min(processes[proc_type]):
		try:
			# Filter by char count and previous count (if exists):
			df = df[df.priv_name_short.str.len() <= proc_fields['char_counts']]
			df = df[df.priv_name_short.str.len() > processes[proc_type][proc_num - 1]['char_counts']]
		except:
			df = df[df.priv_name_short.str.len() <= proc_fields['char_counts']]
	
	#Add process number to column for calculating stats purposes:
	df['process_num'] = str(proc_num)
	if not os.path.exists(os.getcwd() + str(dirs['extract_matches_file'])+ '_' + str(conf_file_num) + '.csv'):
		df.to_csv(os.getcwd() + str(dirs['extract_matches_file']) + '_' + str(conf_file_num) + '.csv')
		return df
	else:
		extracts_file = pd.read_csv(os.getcwd() + str(dirs['extract_matches_file']+ '_' + str(conf_file_num) + '.csv'), index_col=None)
		extracts_file = pd.concat([extracts_file, df],ignore_index=True, sort=True)
		extracts_file.to_csv(os.getcwd() + str(dirs['extract_matches_file']) + '_' + str(conf_file_num) + '.csv', index=False)
		return extracts_file


def calc_matching_stats(clustdf, extractdf, processes, dirs, conf_file_num):
	'''
	For each process outlined in the config file, after each process is completed
	extract the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio values

	:return : None
	:output : a short stats file for each config file for manual comparison to see which is better
	'''

	statdf = pd.DataFrame(columns=['Config_File','Total_Matches', 'Percent_Matches','Optim_Matches','Percent_Precision','Percent_Recall', 'Leven_Dist_Avg'])
	# Overall matches, including poor quality:
	statdf.at[conf_file_num, 'Config_File'] = conf_file_num
	statdf.at[conf_file_num, 'Total_Matches'] = len(clustdf[pd.notnull(clustdf['Org_ID'])])
	statdf.at[conf_file_num,'Percent_Matches'] = round(len(clustdf[pd.notnull(clustdf['Org_ID'])])/len(privdf) * 100, 2)
	# Overall optimised matches :
	statdf.at[conf_file_num,'Optim_Matches'] = len(extractdf)
	# Recall - how many of the selected items are relevant to us?
	statdf.at[conf_file_num,'Percent_Precision'] = round(len(extractdf) / len(clustdf) * 100, 2)
	# Recall - how many relevant items have been selected from the entire original private data 
	statdf.at[conf_file_num,'Percent_Recall'] = round(len(extractdf) / len(privdf) * 100, 2)
	statdf.at[conf_file_num,'Leven_Dist_Avg'] = np.average(extractdf.leven_dist)
	
	# statdf = statdf.transpose()
	
	# if statsfile doesnt exist, create it
	# if not os.path.exists(os.getcwd() + str(dirs['stats_file']) + '_' + str(conf_file_num) + '.csv'):
	if not os.path.exists(os.getcwd() + str(dirs['stats_file']) + '.csv'):
		# statdf.to_csv(os.getcwd() + str(dirs['stats_file']+'_'+ str(conf_file_num) + '.csv'))
		statdf.to_csv(os.getcwd() + str(dirs['stats_file'] + '.csv'))
	# if it does exist, concat current results (if possible in a separate table) with previous
	else:
		# main_stat_file = pd.read_csv(os.getcwd() + str(dirs['stats_file']) + '_' + str(conf_file_num) + '.csv', index_col=None)
		main_stat_file = pd.read_csv(os.getcwd() + str(dirs['stats_file']) + '.csv', index_col=None)
		main_stat_file = pd.concat([main_stat_file, statdf],ignore_index=True, sort=True)
		# main_stat_file.to_csv(os.getcwd() + str(dirs['stats_file']) + '_' + str(conf_file_num) + '.csv', index=False)
		main_stat_file.to_csv(os.getcwd() + str(dirs['stats_file']) + '.csv', index=False)


def manual_matching(dirs, conf_choice):
	'''
	Provides user-input functionality for manual matching based on the extracted records
	:return manual_match_file: extracted file with added column (Y/N/Unsure)
	'''
	
	manual_match_file = pd.read_csv(os.getcwd() + str(dirs['extract_matches_file']+'_'+str(conf_choice) + '.csv'), index_col=None)
	manual_match_file['Manual_Match'] = ''

	choices = ['n', 'na']
	choice = input("\nMatching name only or name and address? (N / NA):")
	while choice.lower() not in choices:
		choice = input("\nMatching name only or name and address? (N / NA):")

	# Iterate over the file, shuffled with sample, as best matches otherwise would show first:
	for index, row in manual_match_file.sample(frac=1).iterrows():
		if choice.lower() == 'n':
			print("\nPrivate name: " + str(row.priv_name_adj))
			print("\nPublic name: " + str(row.pub_name_adj))
			print("\nLevenshtein distance: " + str(row.leven_dist))
		else:
			print("\nPrivate name: " + str(row.priv_name_adj))
			print("Private address: " + str(row.priv_address))
			print("\nPublic name: " + str(row.pub_name_adj))
			print("Public address: " + str(row.pub_address))
			print("\nLevenshtein distance (names): " + str(row.leven_dist))

		match_options = ["y", "n", "u", "f"]
		match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")
		while match.lower() not in match_options:
			match = input("\nMatch? Yes, No, Unsure, Finished (Y/N/U/F):")

		if str(match).lower() != "f":
			manual_match_file.at[index, 'Manual_Match'] = str(match).capitalize()
			continue
		else:
			break

	print("Saving...")
	manual_match_file.to_csv(os.getcwd() + str(dirs['manual_matches_file']) + '_' + str(conf_choice) + '.csv', index=False, \
		columns=['Cluster ID', 'Confidence Score','Org_ID','id', 'leven_dist', 'org_name', 'priv_address','priv_name','priv_name_adj','process_num', 'pub_address','pub_name_adj','Manual_Match'])	
	return manual_match_file

def convert_to_training(config_dirs, man_matched):
	'''
	Converts the manually matched dataframe into a training file for dedupe
	:return : None
	:output : template.json training file
	'''
	
	# Filter for matched entries
	man_matched = man_matched[pd.notnull(man_matched['Manual_Match'])]
	manualdict = {}
	manualdict['distinct'] = []
	manualdict['match'] = []

	# For each row in in the manual matches df, create a sub-dict to be
	# appended to manualdict
	for index, row in man_matched.iterrows():
		new_data = {"__class__" : "tuple", 
						"__value__" : [
										{
									   	"priv_name_adj": str(row.priv_name_adj),
									   	"priv_address" : str(row.priv_address)
									   	}, 
									   {
									   "priv_name_adj": str(row.pub_name_adj),
									   	"priv_address" : str(row.pub_address) 
									   }
									 ]}

		# If the row was a match or not a match, append to 
		# either the match key or the distinct key, respectively:
		if row.Manual_Match == 'Y':
			manualdict['match'].append(new_data)
		elif row.Manual_Match == 'N':
			manualdict['distinct'].append(new_data)
		# If row was 'unsure'd, ignore it as it doesn't contribute to training data
		else:
			continue
	# Write dict to training file:
	with open(os.getcwd() + str(config_dirs['manual_training_file']), 'w') as outfile:
		json.dump(manualdict, outfile)



if __name__ == '__main__':
	#Silence warning for df['process_num'] = str(proc_num)
	pd.options.mode.chained_assignment = None 

	#Define config file variables and related arguments
	config_path = Path('./Config_Files')
	config_dirs = config_dirs.dirs["dirs"]
	
	# Ignores config_dirs - convention is <num>_config.py
	pyfiles = "*_config.py"
	
	try:
		# For each config file read it and convert to dictionary for accessing
		for conf_file in config_path.glob(pyfiles):
			with open(conf_file) as config_file:
				file_contents = []
				file_contents.append(config_file.read())
				# Convert list to dictionary
				configs = ast.literal_eval(file_contents[0])
				conf_file_num = int(conf_file.name[0])
				processes = configs['processes']

				# # Clean public and private datasets for linking
				privdf = clean_private_data(configs, config_dirs)
				pubdf = clean_public_data(configs, config_dirs)
				
				# For each process type ( eg: Name & Add, Name only) outlined in the configs file:
				for proc_type in processes:
					for proc_num in processes[proc_type]:
					#Retrieve the fields for each separate process
						proc_fields = processes[proc_type][proc_num]
						main_proc = min(processes[proc_type].keys())
						# Define data types for clustered file. Enables faster loading.
						clustdtype = {'Cluster ID':np.int, 'Confidence Score': np.float, \
											'id':np.int, 'priv_name':np.str, 'priv_address':np.str, \
											'priv_name_adj':np.str, 'Org_ID':np.str, 'pub_name_adj': np.str, \
											'pub_address':np.str }
							
						# Run dedupe for matching and calculate related stats for comparison
						if not os.path.exists(os.getcwd() + str(processes[proc_type][main_proc]['assigned_output_file'])):
							# if 'dedupe_field_names' in proc_fields:
							dedupe_match_cluster(config_dirs, proc_fields)
							
							clustdf = pd.read_csv(os.getcwd() + str(proc_fields["cluster_output_file"]), index_col=None, \
								dtype=clustdtype)
							# Copy public data to high-confidence cluster records
							clustdf = assign_pub_data_to_clusters(clustdf, os.getcwd() + str(proc_fields['assigned_output_file']))
							clustdf.to_csv(os.getcwd() + str(processes[proc_type][main_proc]['assigned_output_file']), index=False)
						else:
							clustdf = pd.read_csv(str(os.getcwd() + processes[proc_type][main_proc]["assigned_output_file"]), index_col=None, dtype=clustdtype)

						# Remove company suffixes for more relevant levenshtein distance calculation
						clustdf['priv_name_short'] = clustdf.priv_name_adj.apply(shorten_name)
						clustdf['pub_name_short'] = clustdf.pub_name_adj.apply(shorten_name)
						
						#Adds leven_dist column and extract matches based on config process criteria:
						extracts_file = extract_matches(clustdf, proc_fields, config_dirs, proc_num, proc_type, processes, conf_file_num)
					
				calc_matching_stats(clustdf, extracts_file, processes, config_dirs, conf_file_num)
		
	except StopIteration:
		# End program if no more config files found
		print("Done")
	# User defined manual matching:
	conf_choice = input("\nReview Matches_Stats.csv and choose best config file number:")
	man_matched = manual_matching(config_dirs, conf_choice)
	# Convert manual matches to JSON training file.
	man_matched = pd.read_csv(os.getcwd() + str(config_dirs['manual_matches_file']) + '_' + str(conf_choice) + '.csv', usecols=['Manual_Match', 'priv_name_adj', 'priv_address', 'pub_name_adj', 'pub_address'])
	convert_to_training(config_dirs, man_matched)
	print("Process complete - review Manual Matches file.")









