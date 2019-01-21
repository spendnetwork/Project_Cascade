{
"processes":
        {
      "Name_Only":
        {1 :
              {"dedupe_field_names": {
                                      "private_data": ["priv_name_adj"],
                                      "public_data" : ["pub_name_adj"]
                                      },
                "char_counts" : 0,
                "substitutions" : False,
                "min_match_score" : 100,
                "match_output_file": '/Outputs/Name_Only/N_only_matched.csv',
                "match_output_filt_file":'/Outputs/Name_Only/N_only_matched_1.csv',
                "match_training_file":'/Training_Files/Name_Only/Matching/training.json',
                "cluster_training_file":'/Training_Files/Name_Only/Clustering/training.json',
                "cluster_output_file":'/Outputs/Name_Only/N_matched_clustered.csv',
                "assigned_output_file":'/Outputs/Name_Only/N_matched_clust_assigned.csv'
              },
        2 :
              {
                "char_counts" : 3,
                "substitutions" : False,
                "min_match_score" : 95,
              },
        3 : 
             {
              "char_counts" : 6,
              "substitutions" : False,
              "min_match_score" : 90,
            },
         4 : 
             {
              "char_counts" : 9,
              "substitutions" : False,
              "min_match_score" : 85,
            },
          5 : 
             {
              "char_counts" : 12,
              "substitutions" : False,
              "min_match_score" : 80,
            },
          6 : 
             {
              "char_counts" : 15,
              "substitutions" : False,
              "min_match_score" : 80,
            },
          7 : 
             {
              "char_counts" : 18,
              "substitutions" : False,
              "min_match_score" : 75,
            },

        }

        }
      }