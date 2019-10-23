import pandas as pd
import json
from runfile import Main, logging
import pdb
import glob
import os
from collections import defaultdict

class ConvertToTraining(Main):
    def __init__(self, settings):
        super().__init__(settings)


    def convert(self):
        """
        Converts the manually matched dataframe into a training file for dedupe
        :return : None
        :output : training.json training file
        """

        files = glob.glob(os.path.join(self.directories['verified_matches_dir'].format(self.region_dir, self.proc_type),'*'))
        for file in files:
            conversion_file = pd.read_csv(file)

        # Filter for matched entries
            conversion_file = conversion_file[pd.notnull(conversion_file['Manual_Match_N'])]
            manualdict = {}
            manualdict['distinct'] = []
            manualdict['match'] = []

            # For each row in in the manual matches df, create a sub-dict to be
            # appended to manualdict
            for index, row in conversion_file.iterrows():
                new_data = {"__class__": "tuple",
                            "__value__": [
                                {
                                    "src_name": str(row.src_name),
                                    "src_tag": str(row.src_tag),
                                    "src_name_adj": str(row.src_name),
                                    "src_address_adj": str(row.src_address_adj),
                                    "src_joinfields": ''
                                },
                                {
                                    "src_name_adj": str(row.reg_name_adj),
                                }
                            ]}

                # If the row was a match or not a match, append to
                # either the match key or the distinct key, respectively:
                if row.Manual_Match_N == 'Y':
                    manualdict['match'].append(new_data)

                elif row.Manual_Match_N == 'N':
                    manualdict['distinct'].append(new_data)

                else:
                    next

            # Write dict to training file.
            pdb.set_trace()
            with open(self.directories['manual_training_file'].format(self.region_dir, self.proc_type)) as outfile:
                try:
                    # Load TextWrapperIO object into json object
                     data = json.load(outfile)
                except: # If dict empty (training file does not exist)
                    data = defaultdict() # collections.defaultdict will create keys that don't exist on the fly
            try:
                # data[X] is a list within the dictionary, which gives access the 'extend' iterator, to iteratively
                # add each item in manualdict[X] dict (also a list)
                data['distinct'].extend(manualdict['distinct'])
            except AttributeError:
                next
            except KeyError: # If dict empty
                data['distinct'] = manualdict['distinct']

            try:
                data['match'].extend(manualdict['match'])
            except AttributeError:
                next
            except KeyError: # If dict empty
                data['match'] = manualdict['match']

            with open(self.directories['manual_training_file'].format(self.region_dir, self.proc_type), 'w+') as outfile:
                # Overwrite the training file with the outfile, which now contains the old training file plus the new
                json.dump(data, outfile)
