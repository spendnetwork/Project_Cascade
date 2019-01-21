{
"dirs":
      {
      "proj_dir": "/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/",
      "raw_dir": "/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Raw_Data/",
      "adj_dir": "/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Adj_Data/",
      "extract_matches_file": "/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Extracted_Matches/Extracted_Matches.csv",
    "stats_file": "/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Extracted_Matches/Matches_Stats.csv",
      "raw_priv_data": "private_data.csv",
      "raw_pub_data": "public_data.csv",
      "adj_priv_data": "priv_data_adj.csv",
      "adj_pub_data": "pub_data_adj.csv",

      },
"processes":
        {
        "Name_Address":
              {1 :
                    {"dedupe_field_names": {
                                            "private_data": ["priv_name_adj", "priv_address"],
                                            "public_data" : ["pub_name_adj", "pub_address"]
                                            },
                      "char_counts" : None,
                      "substitutions" : False,
                      "min_match_score" : 100,
                      "match_output_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Name_Address/N_A_matched.csv',
                      "match_training_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Training_Files/Name_Address/Matching/training.json',
                      "skip_match_training": False,
                      "cluster_output_file": None,
                      "cluster_training_file": None,
                      "skip_cluster_training": None
                    },

              2 :
                    {
                      "char_counts" : None,
                      "substitutions" : True,
                      "min_match_score" : 100,
                      "skip_match_training" : True, # Training not needed as same training file as p1
                      "cluster_training_file": None,
                      "cluster_output_file": None,
                      "skip_cluster_training": None
                    }
              },
      
      "Name_Only":
        {3 :
              {"dedupe_field_names": {
                                      "private_data": ["priv_name_adj"],
                                      "public_data" : ["pub_name_adj"]
                                      },
                "char_counts" : 0,
                "substitutions" : False,
                "min_match_score" : 100,
                "match_output_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Name_Only/N_only_matched.csv',
                "match_output_filt_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Name_Only/N_only_matched_1.csv',
                "match_training_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Training_Files/Name_Only/Matching/training.json',
                "cluster_training_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Training_Files/Name_Only/Clustering/training.json',
                "cluster_output_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Name_Only/N_matched_clustered.csv',
                "assigned_output_file": '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Outputs/Name_Only/N_matched_clust_assigned.csv',
              },
        4 :
              {
                "char_counts" : 3,
                "substitutions" : False,
                "min_match_score" : 95,
              },
        5 : 
             {
              "char_counts" : 6,
              "substitutions" : False,
              "min_match_score" : 90,
            },
         6 : 
             {
              "char_counts" : 9,
              "substitutions" : False,
              "min_match_score" : 85,
            },
          7 : 
             {
              "char_counts" : 12,
              "substitutions" : False,
              "min_match_score" : 80,
            },
          8 : 
             {
              "char_counts" : 15,
              "substitutions" : False,
              "min_match_score" : 80,
            },
          9 : 
             {
              "char_counts" : 18,
              "substitutions" : False,
              "min_match_score" : 75,
            },

        }

        }
      }