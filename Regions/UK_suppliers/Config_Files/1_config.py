{
    "processes":
        {
            "Name_Only":
                {1:
                    {"dedupe_field_names": {
                        "source_data": ["src_name_adj"],
                        "registry_data": ["reg_name_adj"]
                    },
                        "char_counts": 0,
                        "min_match_score": 100,
                        "recycle_phase": False
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
                    8:
                        {
                            # Keep char count as previous procnum as now looking at anything > than 18, not just 15-18, to capture everything other char length
                            "char_counts": 18,
                            "min_match_score": 50,
                        }

                },
            "Name_Address":
                {1:
                    {"dedupe_field_names": {
                        "source_data": ["src_name_adj", "src_address_adj"],
                        "registry_data": ["reg_name_adj", "reg_address_adj"]
                    },
                        "char_counts": 0,
                        "min_match_score": 100,
                        "recycle_phase": True,
                        "upload_table" : "matching.matches_test",
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
                    8:
                        {
                            # Keep char count as previous procnum as now looking at anything > than 18, not just 15-18, to capture everything other char length
                            "char_counts": 18,
                            "min_match_score": 50,
                        }

                }

        }
}
