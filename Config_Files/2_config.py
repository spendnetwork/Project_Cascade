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
                "min_match_score" : 100,
                "recycle_phase" : False,
              },
        2 :
              {
                "char_counts" : 3,
                "min_match_score" : 90,
              },
        3 :
             {
              "char_counts" : 6,
              "min_match_score" : 85,
            },
         4 :
             {
              "char_counts" : 9,
              "min_match_score" : 80,
            },
          5 :
             {
              "char_counts" : 12,
              "min_match_score" : 75,
            },
          6 :
             {
              "char_counts" : 15,
              "min_match_score" : 70,
            },
          7 :
             {
              "char_counts" : 18,
              "min_match_score" : 65,
            },

        },
            "Name_Address":
                {1:
                    {"dedupe_field_names": {
                        "private_data": ["priv_name_adj", "priv_address"],
                        "public_data": ["pub_name_adj", "pub_address"]
                    },
                        "char_counts": 0,
                        "min_match_score": 100,
                        "recycle_phase": True,
                    },
                    2:
                        {
                            "char_counts": 3,
                            "min_match_score": 90,
                        },
                    3:
                        {
                            "char_counts": 6,
                            "min_match_score": 85,
                        },
                    4:
                        {
                            "char_counts": 9,
                            "min_match_score": 80,
                        },
                    5:
                        {
                            "char_counts": 12,
                            "min_match_score": 75,
                        },
                    6:
                        {
                            "char_counts": 15,
                            "min_match_score": 70,
                        },
                    7:
                        {
                            "char_counts": 18,
                            "min_match_score": 65,
                        },
                }

        }
      }