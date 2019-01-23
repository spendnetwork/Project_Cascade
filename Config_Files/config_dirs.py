import os
from Project_Cascade import proc_type
import pdb

dirs = {
"dirs":
      {
      "raw_dir": os.getcwd() + "/Raw_Data/",
      "adj_dir":  os.getcwd() + "/Adj_Data/",
      "raw_priv_data": "private_data.csv",
      "raw_pub_data": "public_data.csv",
      "adj_priv_data": "priv_data_adj.csv",
      "adj_pub_data": "pub_data_adj.csv",

      "extract_matches_file":os.getcwd() + "/Outputs/Extracted_Matches/Extracted_Matches",
      "manual_matches_file":os.getcwd() + "/Outputs/Extracted_Matches/Manual_Matches",
      "manual_training_file":os.getcwd() + "/Training_Files/Manual_Training/template.json",

      "stats_file":os.getcwd() + "/Outputs/Extracted_Matches/Matches_Stats",

      "cluster_training_file":os.getcwd() + '/Training_Files/' + str(proc_type) + '/Clustering/training.json',
      "cluster_output_file":os.getcwd() + '/Outputs/' + str(proc_type) + '/' str(proc_type) + '_mtchd_clustered.csv',
      "assigned_output_file":os.getcwd() + '/Outputs/' + str(proc_type) + '/' str(proc_type) + 'mtchd_clust_assigned.csv',

      "match_output_file": os.getcwd() + '/Outputs/' + str(proc_type) + '/' + str(proc_type) + '_matched.csv',
      "match_output_filt_file": os.getcwd() + '/Outputs/' + str(proc_type) + '/' + str(proc_type) + '_matched_filt.csv',
      "match_training_file": os.getcwd() + '/Training_Files/' + str(proc_type) + '/Matching/training.json',
  
      "training_files":os.getcwd() + '/Training_Files/',

}}