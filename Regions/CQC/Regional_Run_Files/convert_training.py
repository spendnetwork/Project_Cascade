import pandas as pd
import json
from runfile import Main, logging
import pdb

class ConvertToTraining(Main):
    def __init__(self, settings):
        super().__init__(settings)


    def convert(self):
        """
        Converts the manually matched dataframe into a training file for dedupe
        :return : None
        :output : training.json training file
        """

        self.conv_file = pd.read_csv(
                    self.directories['unverified_matches_file'].format(self.region_dir, self.proc_type) + '_' + str(self.best_config) + '.csv', dtype=self.df_dtypes)
        # Filter for matched entries
        self.conv_file = self.conv_file[pd.notnull(self.conv_file['Manual_Match_NA'])]
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
                                "src_address": str(row.reg_address)
                            }
                        ]}

            # If the row was a match or not a match, append to
            # either the match key or the distinct key, respectively:
            if row.Manual_Match_NA == 'Y':
                manualdict['match'].append(new_data)
            # elif row.Manual_Match_NA == 'N':
            #     manualdict['distinct'].append(new_data)
            # # If row was 'unsure'd, ignore it as it doesn't contribute to training data
            # else:
            #     continue
            elif row.Manual_Match_NA == 'U':
                next

            else:
                manualdict['distinct'].append(new_data)

        # Write dict to training file backup.
        # 'w+' allows writing, and + creates if doesn't exist.
        with open(self.directories['manual_matching_train_backup'].format(self.region_dir), 'w+') as outfile:
            json.dump(manualdict, outfile)