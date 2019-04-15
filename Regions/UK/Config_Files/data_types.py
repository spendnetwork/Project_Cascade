import numpy as np

df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.float, 'priv_name': np.str,
            'priv_name_adj': np.str, 'org_id': np.str, 'pub_name_adj': np.str,'pub_address': np.str,
             'pub_address_adj': np.str,'priv_name_short': np.str, 'pub_name_short': np.str,'leven_dist_N': np.int,
             'org_name': np.str}


dbUpload_cols = ['priv_name', 'CH_name', 'Manual_Match_N', 'company_url', 'CH_id', 'CH_address', 'leven_dist_N']

publicTableSource = None