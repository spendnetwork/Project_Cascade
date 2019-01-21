import pdb
import argparse
import pandas as pd
import os
import subprocess
import re
from pathlib import Path
from fuzzywuzzy import fuzz
from tqdm import tqdm
from time import time
import numpy as np
import math
import glob
import ast
from Config_Files import config_dirs
import sys

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
	load csv, take both org name and address and lower,split, remove punctuation
	id, supplier_name, supplier_streetadd
	'''
	raw_data = config_dirs['raw_dir'] + config_dirs['raw_priv_data']
	adj_data = config_dirs['adj_dir'] + config_dirs['adj_priv_data']

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
	load public csv as per config file args from the correct folder, clean, and then merge address2 and 3 into address 1
	'''
	raw_data = config_dirs['raw_dir'] + config_dirs['raw_pub_data']
	adj_data = config_dirs['adj_dir'] + config_dirs['adj_pub_data']
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


def csvlink_dedupe(dirs,proc_fields):
	'''
	use csvlink to merge public and private data based on config file criteria (name, name&address, split file char length)
	'''
	
	priv_fields = proc_fields['dedupe_field_names']['private_data']
	pub_fields = proc_fields['dedupe_field_names']['public_data']
	priv_file = dirs['adj_dir'] + dirs['adj_priv_data']
	pub_file = dirs['adj_dir'] + dirs['adj_pub_data']
	# Matching:
	if not os.path.exists(proc_fields['match_output_file']):
		print("Starting matching...")
		cmd = ['csvlink '
				+ str(priv_file) + ' '
				+ str(pub_file)
				+ ' --field_names_1 ' + ' '.join(priv_fields) \
				+ ' --field_names_2 ' + ' '.join(pub_fields) \
				+ ' --training_file ' + str(proc_fields['match_training_file']) \
				+ ' --output_file ' + str(proc_fields['match_output_file'])]
		p = subprocess.Popen(cmd, shell=True)
		p.wait() 

		df = pd.read_csv(str(proc_fields['match_output_file']), \
			usecols=['id','priv_name','priv_address','priv_name_adj','Org_ID','pub_name_adj','pub_address'], \
			dtypes = {'id': np.str,'priv_name': np.str,'priv_address': np.str,'priv_name_adj': np.str,'Org_ID': np.str,'pub_name_adj': np.str,'pub_address': np.str})
		df = df[pd.notnull(df['priv_name'])]
		df.drop(columns=['Unnamed: 0', 'Unnamed: 5'], axis=0, inplace=True)
		df.to_csv(str(proc_fields['match_output_filt_file']), index=False)

	# Clustering:
	if not os.path.exists(proc_fields['cluster_output_file']):
		print("Starting clustering...")
		cmd = ['python csvdedupe.py '
				+ str(proc_fields['match_output_filt_file']) + ' '
				+ ' --field_names ' + ' '.join(priv_fields) \
				+ ' --training_file ' + str(proc_fields['cluster_training_file']) \
				+ ' --output_file ' + str(proc_fields['cluster_output_file'])]
		p = subprocess.Popen(cmd, cwd='./csvdedupe/csvdedupe', shell=True)
		p.wait() 
	else:	
		pass

def shorten_name(row):
		row = str(row).replace('-',' ').replace("  "," ").strip()
		rowsplit = str(row).split(" ")
		for i in rowsplit:
			if i in org_type_dict.values():
				rowadj = row.replace(i, '').strip()
		try:
			return rowadj
		except:
			return row

def assign_org_ids_to_clusters(df, assigned_file):
    '''
    Members of a cluster they will be assigned the obtained id number of the highest-confidence
    row in that cluster. At this stage the amount of confidence is irrelevant as these will be measured
    during the match extraction phase.
    '''
    st = set(df['Cluster ID'])
    df.sort_values(by=['Cluster ID'], inplace=True, axis=0, ascending=True)
    df.reset_index(drop=True, inplace=True)
    tqdm.pandas()
    df = df.groupby(['Cluster ID']).progress_apply(get_max_id)
    df.to_csv(assigned_file, index=False)
    return df
    

def get_max_id(group):
	max_conf_idx = group['Confidence Score'].idxmax()
	for index, row in group.iterrows():
		if pd.isnull(row.Org_ID):
			group.at[index, 'Org_ID'] =  group['Org_ID'][max_conf_idx]
			group.at[index, 'pub_name_adj'] =  group['pub_name_adj'][max_conf_idx]
			group.at[index, 'pub_address'] =  group['pub_address'][max_conf_idx]
	return group


def calc_match_ratio(row):
	if pd.notnull(row.priv_name_short) and pd.notnull(row.pub_name_short):
		return fuzz.ratio(row.priv_name_short, row.pub_name_short)



def extract_matches(df, proc_fields, dirs, proc_num, proc_type, processes, conf_file_num):
	'''
	import config file containing variable assignments for i.e. char length, match ratio
	using these, calculate stats of the number of matches for each lev ratio criteria range
	'''
	# Add levenshtein matching ratio
	if 'leven_dist' not in df.columns:
		df['leven_dist'] = df.apply(calc_match_ratio, axis=1)
	# If the process has a minimum string length, filter by the range of this process 
	# and the previous process minimum length and the minimum match score

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
	if not os.path.exists(dirs['extract_matches_file']+ '_' + str(conf_file_num) + '.csv'):
		df.to_csv(dirs['extract_matches_file']+ '_' + str(conf_file_num) + '.csv')
		return df
	else:
		extracts_file = pd.read_csv(str(dirs['extract_matches_file']+ '_' + str(conf_file_num) + '.csv'), index_col=None)
		extracts_file = pd.concat([extracts_file, df],ignore_index=True, sort=True)
		extracts_file.to_csv(dirs['extract_matches_file']+ '_' + str(conf_file_num) + '.csv', index=False)
		return extracts_file


def calc_matching_stats(clustdf, extractdf, processes, dirs, conf_file_num):
	'''
	for each process outlined in the config file, after each process is completed
	extract the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio columns set above
	'''
	statdf = pd.DataFrame(columns=['Config_File','Total_Matches', 'Percent_Matches','Optim_Matches','Percent_Precision','Percent_Recall', 'Leven_Dist_WAvg'])
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
	statdf.at[conf_file_num,'Leven_Dist_Avg'] = extractdf.groupby(extractdf.process_num).apply(lambda grp: np.average(grp.leven_dist))
	
	statdf = statdf.transpose()
	# if statsfile doesnt exist, create it
	if not os.path.exists(dirs['stats_file']+'_'+ str(proc_num) + '.csv'):
		statdf['Combination_num'] = 1
		statdf.to_csv(dirs['stats_file']+'_'+ str(proc_num) + '.csv')
	# if it does exist, concat current results (if possible in a separate table) with previous
	else:
		main_stat_file = pd.read_csv(str(dirs['stats_file']+'_'+str(proc_num) + '.csv'), index_col=None)
		statdf['Combination_num'] = max(main_stat_file['Combination_num']) + 1
		main_stat_file = pd.concat([main_stat_file, statdf],ignore_index=True, sort=True)
		main_stat_file.to_csv(dirs['stats_file']+'_'+str(proc_num) + '.csv', index=False)


def manual_matching(dirs):
	# pdb.set_trace()
	conf_num = input("\nChoose best config file number:")

	manual_match_file = pd.read_csv(str(dirs['extract_matches_file']+'_'+str(conf_num) + '.csv'), index_col=None)
	manual_match_file['Manual_Match'] = ''

	choices = ['n', 'na']
	choice = input("\nMatching name only or name and address? (N / NA):")
	while choice.lower() not in choices:
		choice = input("\nMatching name only or name and address? (N / NA):")

	for index, row in manual_match_file.iterrows():
		if choice.lower() == 'n':
			print("\nPrivate name: " + str(row.priv_name))
			print("\nPublic name: " + str(row.org_name))
		else:
			print("\nPrivate name: " + str(row.priv_name))
			print("Private address: " + str(row.priv_address))
			print("\nPublic name: " + str(row.org_name))
			print("Public address: " + str(row.pub_address))
		
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
	manual_match_file.to_csv(dirs['manual_matches_file']+ '_' + str(conf_num) + '.csv', index=False)	
	return manual_match_file
	
	






if __name__ == '__main__':
	# Assign classes and sub-functions for clarity OOP
	#Define config file variables and related arguments
	config_path = Path('./Config_Files')
	config_dirs = config_dirs.dirs["dirs"]
	# Ignores config_dirs - convention is <num>_config.py
	pyfiles = "*_config.py"
	# Access Config_Files folder, and add contents of each config file to list:
	try:
		for conf_file in config_path.glob(pyfiles):
			with open(conf_file) as config_file:
				file_contents = []
				file_contents.append(config_file.read())
				# Convert list to dictionary
				configs = ast.literal_eval(file_contents[0])
				conf_file_num = int(conf_file.name[0])
				processes = configs['processes']

				# # Clean datasets for linking
				pubdf = clean_public_data(configs, config_dirs)
				privdf = clean_private_data(configs, config_dirs)
				
				# For each process type (Name & Add, Name only) outlined in the configs file:
				# for proc_type in processes:
				proc_type = 'Name_Only'
				for proc_num in processes[proc_type]:
					#Retrieve the fields for each separate process
					proc_fields = processes[proc_type][proc_num]
					main_proc = min(processes[proc_type].keys())
						
					# Run dedupe for matching and calculate related stats for comparison
					if 'dedupe_field_names' in proc_fields:
						csvlink_dedupe(config_dirs, proc_fields)
						clustdf = pd.read_csv(str(proc_fields["cluster_output_file"]), index_col=None)
						# clustdf = assign_org_ids_to_clusters(clustdf, proc_fields['assigned_output_file'])
					else:
						clustdf = pd.read_csv(str(processes[proc_type][main_proc]["assigned_output_file"]), index_col=None)

					clustdf['priv_name_short'] = clustdf.priv_name_adj.apply(shorten_name)
					clustdf['pub_name_short'] = clustdf.pub_name_adj.apply(shorten_name)
					
					#Adds leven_dist column and extract matches based on config process criteria:
					extract_matches(clustdf, proc_fields, config_dirs, proc_num, proc_type, processes, conf_file_num)
				# calc_matching_stats(clustdf, extracts_file, processes, config_dirs, conf_file_num)
		
	except StopIteration:
		# End program if no more config files found
		print("Done")
	# User defined manual matching:
	man_matched = manual_matching(config_dirs)
	# Convert manual matches to JSON training file.
	convert_to_training(config_dirs, man_matched)