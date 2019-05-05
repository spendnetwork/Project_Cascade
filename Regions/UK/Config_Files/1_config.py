{
    "processes":
        {
            "Name_Only":
                {1:
                    {"dedupe_field_names": {
                        "source_data": ["src_name_short"],
                        "registry_data": ["CH_name_short"]
                    },
                        "char_counts": 0,
                        "min_match_score": 100,
                        "recycle_phase": False,
                        # "db_table" : 'matching.gb_coh'
                        "db_table" : 'matching.testtable',

                    },
                    2:
                        {
                            "char_counts": 3,
                            "min_match_score": 95,
                        },
                    3:
                        {
                            "char_counts": 6,
                            "min_match_score": 90,
                        },
                    4:
                        {
                            "char_counts": 9,
                            "min_match_score": 85,
                        },
                    5:
                        {
                            "char_counts": 12,
                            "min_match_score": 80,
                        },
                    6:
                        {
                            "char_counts": 15,
                            "min_match_score": 80,
                        },
                    7:
                        {
                            "char_counts": 18,
                            "min_match_score": 75,
                        },

                }
        }
}
