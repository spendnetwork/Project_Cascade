import numpy as np
import directories
from Regions.UK_entities import Regional_Run_Files as UK_ent_run_files
from Regions.UK_suppliers import Regional_Run_Files as UK_sup_run_files


class UK_entities():
    proc_type = 'Name_Only'
    directories = directories.dirs["dirs"]
    # Need to define proc_num here otherwise will not be carried through as part of 'self'
    proc_num = int
    conf_file_num = int
    configs = dict
    main_proc = int
    upload_table = str
    transfer_table = str
    best_config = int

    training_cols = ['src_name', 'CH_name', 'Manual_Match_N', 'CH_id', 'CH_address', 'leven_dist_N']
    df_dtypes = {'Cluster_ID': np.int, 'Confidence_Score': np.float,
                 'src_name': np.str, 'src_name_adj': np.str, 'src_streetaddress': np.str, 'src_streetaddress_adj': np.str, 'src_name_short': np.str,
                 'src_address_locality': np.str, 'src_address_postalcode': np.str, 'src_address_streetaddress': np.str,
                 'reg_id': np.str, 'reg_name_adj': np.str, 'reg_address': np.str, 'reg_address_adj': np.str,  'reg_name_short': np.str,
                 'leven_dist_N': np.int, 'leven_dist_NA': np.int,
                 'Manual_Match_N':np.str,  'Manual_Match_NA': np.str,
                 'src_joinfields':np.str, 'reg_joinfields': np.str, 'src_tag':np.str,
                 'match_source': np.str,'reg_scheme':np.str, 'src_address_adj': np.str,
                 'match_by': np.str, 'match_date':np.str
                 }

    stats_cols = ['Config_File', 'Total_Src_Rows','Original_Matches', 'Pct_Orig_Matches', 'Filtered_Matches', 'Pct_Filtered_Matches', 'Pct_Precision',
                  'Pct_Recall', 'Leven_Dist_Avg']

    runfile_mods = UK_ent_run_files

    # reg_data_source = 'uk_data.entity'
    reg_data_source = 'ocds.orgs_ocds'
    src_data_source = 'ocds.ocds_tenders_view'
    upload_table = 'matching.uk_entities'
    # transfer_table = 'matching.test_orgs_lookup'
    transfer_table = 'ocds.orgs_lookup'

    dedupe_cols = ['src_name','src_tag', 'src_name_adj', 'src_address_adj',
                   'reg_id', 'reg_name', 'reg_name_adj', 'reg_address', 'reg_address_adj','src_joinfields', 'reg_joinfields',
                   'match_source', 'reg_scheme']

    raw_src_data_cols = ['src_name','src_tag','src_address_locality','src_address_postalcode', 'src_address_countryname','src_address_streetaddress']

    dbUpload_cols = ['src_name', 'reg_name','leven_dist_N',  'Manual_Match_N', 'src_address_adj', 'reg_address_adj', 'Manual_Match_NA', 'leven_dist_NA', 'reg_id',  'src_tag', 'src_id', 'match_source', 'reg_scheme','match_date', 'match_by']

    manual_matches_cols = [ 'src_name','src_tag',  'reg_name', 'Manual_Match_N', 'leven_dist_N',
                           'src_address_adj', 'reg_address_adj', 'Manual_Match_NA', 'leven_dist_NA',
                            'src_name_short',
                           'reg_name_short',
                            'reg_id',
                            'src_id',
                            'reg_name_adj',
                            'src_joinfields',
                            'reg_joinfields',
                            'reg_source',

                            'reg_scheme',
                            'match_by',
                            'match_date'
                            ]

class UK_suppliers():
    proc_type = 'Name_Only'
    directories = directories.dirs["dirs"]
    # Need to define proc_num here otherwise will not be carried through as part of 'self'
    proc_num = int
    conf_file_num = int
    configs = dict
    main_proc = int
    upload_table = str
    transfer_table = str
    best_config = int

    training_cols = ['src_name', 'CH_name', 'Manual_Match_N', 'CH_id', 'CH_address', 'leven_dist_N']

    df_dtypes = {'Cluster_ID': np.int, 'Confidence_Score': np.float,
                 'src_name': np.str, 'src_name_adj': np.str, 'src_name_short': np.str,
                 'reg_id': np.str, 'reg_name_adj': np.str, 'reg_name_short': np.str,
                 'leven_dist_N': np.int, 'leven_dist_NA': np.int,
                 'Manual_Match_N':np.str,  'Manual_Match_NA': np.str,
                 'src_joinfields':np.str, 'reg_joinfields': np.str, 'src_tag':np.str,
                 'match_source': np.str,'reg_scheme':np.str,
                 'match_by': np.str, 'match_date':np.str
                 }

    stats_cols = ['Config_File', 'Total_Src_Rows','Original_Matches', 'Pct_Orig_Matches', 'Filtered_Matches', 'Pct_Filtered_Matches', 'Pct_Precision',
                  'Pct_Recall', 'Leven_Dist_Avg']

    runfile_mods = UK_sup_run_files

    # reg_data_source = 'uk_data.entity'
    reg_data_source = 'ocds.orgs_ocds'
    src_data_source = 'ocds.ocds_awards_suppliers_uk_view'
    upload_table = 'matching.uk_suppliers'
    # transfer_table = 'matching.test_orgs_lookup'
    transfer_table = 'ocds.orgs_lookup'

    dedupe_cols = ['src_name','src_tag', 'src_name_adj', 'src_address_adj',
                   'reg_id', 'reg_name', 'reg_name_adj', 'reg_address', 'reg_address_adj','src_joinfields', 'reg_joinfields',
                   'match_source', 'reg_scheme']

    raw_src_data_cols = ['src_name','src_tag','src_address_locality','src_address_postalcode', 'src_address_countryname','src_address_streetaddress']

    dbUpload_cols = ['src_name', 'reg_name','leven_dist_N',  'Manual_Match_N', 'src_address_adj', 'reg_address_adj', 'Manual_Match_NA', 'leven_dist_NA', 'reg_id',  'src_tag', 'src_id', 'match_source', 'reg_scheme','match_date', 'match_by']

    manual_matches_cols = [ 'src_name','src_tag',  'reg_name', 'Manual_Match_N', 'leven_dist_N',
                           'src_address_adj', 'reg_address_adj', 'Manual_Match_NA', 'leven_dist_NA',
                            'src_name_short',
                           'reg_name_short',
                            'reg_id',
                            'src_id',
                            'reg_name_adj',
                            'src_joinfields',
                            'reg_joinfields',
                            'reg_source',
                            'reg_scheme',
                            'match_by',
                            'match_date'
                            ]

