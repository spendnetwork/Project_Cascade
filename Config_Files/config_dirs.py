import os

dirs = {
"dirs":
      {
      "raw_dir": os.getcwd() + "/Raw_Data/",
      "adj_dir":  os.getcwd() + "/Adj_Data/",
      # "raw_priv_data": "private_data.csv",
      # "raw_pub_data": "public_data.csv",
      "raw_priv_data": "{0}", # Defined by argument parses (default private_data.csv)
      "raw_pub_data": "{0}", # Defined by argument parses (default public_data.csv)

      "adj_priv_data": "priv_data_adj.csv",
      "adj_pub_data": "pub_data_adj.csv",
      "proc_type_dir" : os.getcwd() + '/Outputs/{0}',
      "proc_type_train_dir" : os.getcwd() + '/Training_Files/{0}',
      "proc_type_train_clust_dir" : os.getcwd() + '/Training_Files/{0}/Clustering,
      "proc_type_train_match_dir" : os.getcwd() + '/Training_Files/{0}/Matching,
      "match_training_file": os.getcwd() + '/Training_Files/{0}/Matching/training.json',
      "match_output_file": os.getcwd() + '/Outputs/{0}/{0}_matched.csv',

      "cluster_training_file":os.getcwd() + '/Training_Files/{0}/Clustering/training.json',
      "cluster_output_file":os.getcwd() + '/Outputs/{0}/{0}_mtchd_clustered.csv',

      "assigned_output_file":os.getcwd() + '/Outputs/{0}/{0}_mtchd_clust_assigned.csv',

      "extract_matches_file":os.getcwd() + "/Outputs/Extracted_Matches/Extracted_Matches",
      "stats_file":os.getcwd() + "/Outputs/Extracted_Matches/Matches_Stats",

      "manual_matches_file":os.getcwd() + "/Outputs/Extracted_Matches/Manual_Matches",
      "manual_training_file":os.getcwd() + "/Training_Files/Manual_Training/template.json",
}}