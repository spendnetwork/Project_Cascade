import pandas as pd
import os
import numpy as np
import pdb
from runfile import Main, logging

class StatsCalculations(Main):

    """
	For each process outlined in the config file, after each process is completed
	verify the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio values

	:return : None
	:output : a short stats file for each config file for manual comparison to see which is better
	"""
    def __init__(self, settings, clustdf, extractdf, srcdf):
        Main.__init__(self, settings)
        self.clustdf = clustdf
        self.extractdf = extractdf
        self.srcdf = srcdf


    def calculate(self):
        # Remove old stats file if exists and if first iteration over config files:
        if os.path.exists(self.directories['stats_file'].format(self.region_dir, self.proc_type)):
            if self.conf_file_num == 1:
                os.remove(self.directories['stats_file'].format(self.region_dir, self.proc_type))

        statdf = pd.DataFrame(columns=self.stats_cols)

        # Overall matches, including poor quality:
        statdf.at[self.conf_file_num, 'Config_File'] = self.conf_file_num
        statdf.at[self.conf_file_num, 'Total_Matches'] = len(self.clustdf[pd.notnull(self.clustdf['CH_id'])])
        statdf.at[self.conf_file_num, 'Percent_Matches'] = round(len(self.clustdf[pd.notnull(self.clustdf['CH_id'])]) / len(self.srcdf) * 100,2)
        # Overall optimised matches :
        statdf.at[self.conf_file_num, 'Optim_Matches'] = len(self.extractdf)
        # Precision - how many of the selected items are relevant to us? (TP/TP+FP)
        # This is the size of the extracted matches divided by the total number of
        statdf.at[self.conf_file_num, 'Percent_Precision'] = round(len(self.extractdf) / len(self.clustdf) * 100, 2)
        # Recall - how many relevant items have been selected from the entire original source data (TP/TP+FN)
        statdf.at[self.conf_file_num, 'Percent_Recall'] = round(len(self.extractdf) / len(self.srcdf) * 100, 2)

        if self.in_args.recycle:
            statdf.at[self.conf_file_num, 'Leven_Dist_Avg'] = np.average(self.extractdf.leven_dist_NA)
        else:
            statdf.at[self.conf_file_num, 'Leven_Dist_Avg'] = np.average(self.extractdf.leven_dist_N)
        # if statsfile doesnt exist, create it
        if not os.path.exists(self.directories['stats_file'].format(self.region_dir, self.proc_type)):
            statdf.to_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type, index=False))
        # if it does exist, concat current results with previous
        else:
            main_stat_file = pd.read_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type), index_col=None)
            main_stat_file = pd.concat([main_stat_file, statdf], ignore_index=True, sort=True)
            main_stat_file.to_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type), index=False,
                                  columns=self.stats_cols)
            return main_stat_file
