import pandas as pd
import json
from Config_Files import config_dirs
from run import get_input_args


def convert_to_training(config_dirs, man_matched):
    """
	Converts the manually matched dataframe into a training file for dedupe
	:return : None
	:output : training.json training file
	"""

    # Filter for matched entries
    man_matched = man_matched[pd.notnull(man_matched['Manual_Match'])]
    manualdict = {}
    manualdict['distinct'] = []
    manualdict['match'] = []

    # For each row in in the manual matches df, create a sub-dict to be
    # appended to manualdict
    for index, row in man_matched.iterrows():
        new_data = {"__class__": "tuple",
                    "__value__": [
                        {
                            "priv_name_adj": str(row.priv_name_adj),
                            "priv_address": str(row.priv_address)
                        },
                        {
                            "priv_name_adj": str(row.pub_name_adj),
                            "priv_address": str(row.pub_address)
                        }
                    ]}

        # If the row was a match or not a match, append to
        # either the match key or the distinct key, respectively:
        if row.Manual_Match == 'Y':
            manualdict['match'].append(new_data)
        elif row.Manual_Match == 'N':
            manualdict['distinct'].append(new_data)
        # If row was 'unsure'd, ignore it as it doesn't contribute to training data
        else:
            continue
    # Write dict to training file backup.
    # 'w+' allows writing, and + creates if doesn't exist.
    with open(config_dirs['manual_matching_train_backup'], 'w+') as outfile:
        json.dump(manualdict, outfile)


if __name__ == '__main__':
    in_args = get_input_args()

    # If initial round of processing, create manual training file:
    if not in_args.recycle:
        # Convert manual matches to JSON training file.
        convert_to_training(config_dirs, man_matched)

    # Filter manual matches file and output to separate csv as confirmed matches
    confirmed_matches = man_matched[man_matched['Manual_Match'] == 'Y']
    confirmed_matches.to_csv(config_dirs['confirmed_matches_file'].format(proc_type), index=False)

    print("Done.")
