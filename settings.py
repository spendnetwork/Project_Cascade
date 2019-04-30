import numpy as np
import directories
from Regions.Italy import Regional_Run_Files as ITA_run_files
from Regions.UK import Regional_Run_Files as UK_run_files
import pdb


class Italy_settings():

    runfile_mods = ITA_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'src_name': np.str,
                 'src_address': np.str,
                 'src_address_adj': np.str, 'src_name_adj': np.str, 'reg_id': np.str, 'reg_name_adj': np.str,
                 'reg_address': np.str,
                 'reg_address_adj': np.str, 'src_name_short': np.str, 'reg_name_short': np.str, 'leven_dist_N': np.int,
                 'leven_dist_NA': np.int,
                 'reg_name': np.str, 'srcjoinfields': np.str, 'regjoinfields': np.str}

    training_cols = ['src_name_adj', 'src_address_adj', 'reg_name_adj', 'reg_address_adj', 'Manual_Match_N',
                     'Manual_Match_NA']

    dbUpload_cols = ['src_name', 'src_address', 'reg_id', 'reg_name', 'reg_address', 'Manual_Match_N',
                     'Manual_Match_NA']

    registryTableSource = "spaziodati.sd_sample"

    directories = directories.dirs["dirs"]




class UK_settings():
    # pdb.set_trace()
    # def __init__(self):
    #     self.configs = None
    #     self.proc_type = None
    #     self.directories = directories
    #     self.regiondir = None
    #     self.runfile_mods = None
    #     self.in_args = None
    #


    runfile_mods = UK_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'src_name': np.str,
             'src_name_adj': np.str, 'CH_id': np.str, 'CH_name_adj': np.str,
                 'CH_address': np.str, 'src_name_short': np.str, 'CH_name_short': np.str, 'leven_dist_N': np.int,
                 'reg_name': np.str, 'home_page_text' : np.str, 'about_or_contact_text' : np.str}

    training_cols = ['src_name', 'CH_name', 'Manual_Match_N', 'company_url', 'CH_id', 'CH_address', 'leven_dist_N']

    manual_matches_cols = ['src_name', 'CH_name', 'Manual_Match_N', 'about_or_contact_text', 'company_url', 'home_page_text', 'CH_id',
     'CH_address', 'src_name_short', 'CH_name_short', 'leven_dist_N']

    registryTableSource = None

    directories = directories.dirs["dirs"]



    