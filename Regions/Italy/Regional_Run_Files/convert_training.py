import pandas as pd
import json
import pdb

class convertToTraining:
    def __init__(self, settings, conv_file):
        self.region_dir = settings.region_dir
        self.directories = settings.directories
        self.conv_file = conv_file

    def convert(self):
        """
        Converts the manually matched dataframe into a training file for dedupe
        :return : None
        :output : training.json training file
        """

        # Filter for matched entries
        conv_file = self.conv_file[pd.notnull(self.conv_file['Manual_Match_NA'])]
        manualdict = {}
        manualdict['distinct'] = []
        manualdict['match'] = []

        # For each row in in the manual matches df, create a sub-dict to be
        # appended to manualdict
        for index, row in self.conv_file.iterrows():
            new_data = {"__class__": "tuple",
                        "__value__": [
                            {
                                "src_name_adj": str(row.src_name_adj),
                                "src_address": str(row.src_address_adj)
                            },
                            {
                                "src_name_adj": str(row.reg_name_adj),
                                "src_address": str(row.reg_address_adj)
                            }
                        ]}

            # If the row was a match or not a match, append to
            # either the match key or the distinct key, respectively:
            if row.Manual_Match_NA == 'Y':
                manualdict['match'].append(new_data)
            elif row.Manual_Match_NA == 'N':
                manualdict['distinct'].append(new_data)
            # If row was 'unsure'd, ignore it as it doesn't contribute to training data
            else:
                continue
        # Write dict to training file backup.
        # 'w+' allows writing, and + creates if doesn't exist.
        with open(self.directories['manual_matching_train_backup'].format(self.region_dir), 'w+') as outfile:
            json.dump(manualdict, outfile)