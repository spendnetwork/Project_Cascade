import numpy as np
import directories
from Regions.Italy import Regional_Run_Files as ITA_run_files
from Regions.UK import Regional_Run_Files as UK_run_files


class Italy_settings():

    runfilemods = ITA_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'priv_name': np.str,
                 'priv_address': np.str,
                 'priv_address_adj': np.str, 'priv_name_adj': np.str, 'org_id': np.str, 'pub_name_adj': np.str,
                 'pub_address': np.str,
                 'pub_address_adj': np.str, 'priv_name_short': np.str, 'pub_name_short': np.str, 'leven_dist_N': np.int,
                 'leven_dist_NA': np.int,
                 'org_name': np.str, 'privjoinfields': np.str, 'pubjoinfields': np.str}

    training_cols = ['priv_name_adj', 'priv_address_adj', 'pub_name_adj', 'pub_address_adj', 'Manual_Match_N',
                     'Manual_Match_NA']

    dbUpload_cols = ['priv_name', 'priv_address', 'org_id', 'org_name', 'pub_address', 'Manual_Match_N',
                     'Manual_Match_NA']

    publicTableSource = "spaziodati.sd_sample"

    directories = directories.dirs["dirs"]



class UK_settings():

    runfilemods = UK_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'priv_name': np.str,
             'priv_name_adj': np.str, 'org_id': np.str, 'pub_name_adj': np.str, 'pub_address': np.str,
                 'pub_address_adj': np.str, 'priv_name_short': np.str, 'pub_name_short': np.str, 'leven_dist_N': np.int,
                 'org_name': np.str}

    training_cols = ['priv_name', 'CH_name', 'Manual_Match_N', 'company_url', 'CH_id', 'CH_address', 'leven_dist_N']

    manual_matches_cols = ['priv_name', 'CH_name', 'Manual_Match_N', 'about_or_contact_text', 'company_url', 'home_page_text', 'CH_id',
     'CH_address', 'priv_name_short', 'CH_name_short', 'leven_dist_N']

    publicTableSource = None



    