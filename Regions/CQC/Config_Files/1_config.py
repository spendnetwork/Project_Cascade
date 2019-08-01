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
                        "recycle_phase": False,
                        "upload_table" : "matching.None",
                    },
                2:
                    {
                        "char_counts": 1000,
                        "min_match_score": 97
                    },
                3: # For CQC we only want >=97. The last process number is used to capture all remaining strings greater than
                # the char length below only - therefore an excessive one means this last process has no effect.
                    {
                        "char_counts": 10000,
                        "min_match_score": 97
                    }
                }
        }
}