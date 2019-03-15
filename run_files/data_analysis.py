import pandas as pd
import os
import numpy as np


def calc_matching_stats(clustdf, extractdf, config_dirs, conf_file_num, proc_type, privdf):
    """
	For each process outlined in the config file, after each process is completed
	extract the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio values

	:return : None
	:output : a short stats file for each config file for manual comparison to see which is better
	"""
    # Remove old stats file if exists and if first iteration over config files:
    if os.path.exists(config_dirs['stats_file'].format(proc_type)):
        if conf_file_num == 1:
            os.remove(config_dirs['stats_file'].format(proc_type))

    statdf = pd.DataFrame(
        columns=['Config_File', 'Total_Matches', 'Percent_Matches', 'Optim_Matches', 'Percent_Precision',
                 'Percent_Recall', 'Leven_Dist_Avg'])
    # Overall matches, including poor quality:
    statdf.at[conf_file_num, 'Config_File'] = conf_file_num
    statdf.at[conf_file_num, 'Total_Matches'] = len(clustdf[pd.notnull(clustdf['org_id'])])
    statdf.at[conf_file_num, 'Percent_Matches'] = round(len(clustdf[pd.notnull(clustdf['org_id'])]) / len(privdf) * 100,
                                                        2)
    # Overall optimised matches :
    statdf.at[conf_file_num, 'Optim_Matches'] = len(extractdf)
    # Precision - how many of the selected items are relevant to us? (TP/TP+FP)
    # This is the size of the extracted matches divided by the total number of
    statdf.at[conf_file_num, 'Percent_Precision'] = round(len(extractdf) / len(clustdf) * 100, 2)
    # Recall - how many relevant items have been selected from the entire original private data (TP/TP+FN)
    statdf.at[conf_file_num, 'Percent_Recall'] = round(len(extractdf) / len(privdf) * 100, 2)
    statdf.at[conf_file_num, 'Leven_Dist_Avg'] = np.average(extractdf.leven_dist)
    # if statsfile doesnt exist, create it
    if not os.path.exists(config_dirs['stats_file'].format(proc_type)):
        statdf.to_csv(config_dirs['stats_file'].format(proc_type))
    # if it does exist, concat current results with previous
    else:
        main_stat_file = pd.read_csv(config_dirs['stats_file'].format(proc_type), index_col=None)
        main_stat_file = pd.concat([main_stat_file, statdf], ignore_index=True, sort=True)
        main_stat_file.to_csv(config_dirs['stats_file'].format(proc_type), index=False,
                              columns=['Config_File', 'Leven_Dist_Avg', 'Optim_Matches', 'Percent_Matches',
                                       'Percent_Precision', 'Percent_Recall', 'Total_Matches'])
        return main_stat_file
