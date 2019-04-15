import numpy as np

df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'priv_name': np.str, 'priv_address': np.str,
             'priv_address_adj': np.str,'priv_name_adj': np.str, 'org_id': np.str, 'pub_name_adj': np.str,'pub_address': np.str,
             'pub_address_adj': np.str,'priv_name_short': np.str, 'pub_name_short': np.str,'leven_dist_N': np.int,'leven_dist_NA': np.int,
             'org_name': np.str, 'privjoinfields': np.str, 'pubjoinfields':np.str}

training_cols = ['priv_name_adj', 'priv_address_adj', 'pub_name_adj', 'pub_address_adj', 'Manual_Match_N','Manual_Match_NA']

dbUpload_cols = ['priv_name', 'priv_address', 'org_id', 'org_name', 'pub_address', 'Manual_Match_N','Manual_Match_NA']

publicTableSource = "spaziodati.sd_sample"