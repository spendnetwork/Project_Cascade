import os

dirs = {
"dirs":
      {
      "raw_dir": os.getcwd() + "/Data_Inputs/Raw_Data/",
      "adj_dir":  os.getcwd() + "/Data_Inputs/Adj_Data/",
      "raw_priv_data": "{0}",
      "raw_pub_data": "{0}",

      "adj_priv_data": "{0}",
      "adj_pub_data": "{0}",
      "proc_type_dir" : os.getcwd() + '/Outputs/{0}',
      "proc_type_matches_dir" : os.getcwd() + '/Outputs/{0}/Deduped_Data/Extracted_Matches',
      "proc_type_train_dir" : os.getcwd() + '/Data_Inputs/Training_Files/{0}',
      "proc_type_train_clust_dir" : os.getcwd() + '/Data_Inputs/Training_Files/{0}/Clustering',
      "proc_type_train_match_dir" : os.getcwd() + '/Data_Inputs/Training_Files/{0}/Matching',
      "match_output_file": os.getcwd() + '/Outputs/{0}/Deduped_Data/{0}_matched.csv',

      "cluster_training_file":os.getcwd() + '/Data_Inputs/Training_Files/{0}/Clustering/cluster_training.json',
      "cluster_output_file":os.getcwd() + '/Outputs/{0}/Deduped_Data/{0}_matched_clustered.csv',

      "assigned_output_file":os.getcwd() + '/Outputs/{0}/Deduped_Data/{0}_matched_clust_assigned.csv',

      "extract_matches_file":os.getcwd() + "/Outputs/{0}/Extracted_Matches/Extracted_Matches",
      "stats_file":os.getcwd() + "/Outputs/{0}/Extracted_Matches/Matches_Stats_{0}.csv",

      "manual_matches_file":os.getcwd() + "/Outputs/{0}/Confirmed_Matches/Manual_Matches",
      "manual_training_file":os.getcwd() + "/Data_Inputs/Training_Files/{0}/Matching/matching_training.json",
      "manual_matching_train_backup":os.getcwd() + "/Data_Inputs/Training_Files/Manual_&_Backups/manual_matching_training.json",
      "backups_dir":os.getcwd() + "/Data_Inputs/Training_Files/Manual_&_Backups",
      "cluster_training_backup":os.getcwd() + "/Data_Inputs/Training_Files/Manual_&_Backups/cluster_training_backup.json",
      "confirmed_matches_file":os.getcwd() + "/Outputs/{0}/Confirmed_Matches/Confirmed_Matches.csv"
}}