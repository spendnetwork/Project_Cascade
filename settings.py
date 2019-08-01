import numpy as np
import directories
from Regions.Italy import Regional_Run_Files as ITA_run_files
from Regions.UK import Regional_Run_Files as UK_run_files
from Regions.UK_entities import Regional_Run_Files as UK_ent_run_files
from Regions.CQC import Regional_Run_Files as CQC_run_files


class Italy_Settings:

    runfile_mods = ITA_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'id': np.str, 'src_name': np.str,
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

    # Needed to kick start the setup dirs function
    proc_type = 'Name_Only'

    dedupe_cols = ['id', 'src_name', 'src_address', 'src_name_adj', 'src_address_adj', 'reg_id',
                    'reg_name',
                    'reg_name_adj', 'reg_address_adj',
                    'reg_address', 'reg_address_adj', 'srcjoinfields', 'regjoinfields']

    stats_cols = ['Config_File', 'Total_Matches', 'Percent_Matches', 'Optim_Matches', 'Percent_Precision',
                     'Percent_Recall', 'Leven_Dist_Avg']

    manual_matches_cols = ['Cluster ID', 'leven_dist_N', 'leven_dist_NA', 'reg_id', 'id', 'reg_name', 'reg_name_adj',
                                           'reg_address', 'src_name', 'src_name_adj', 'src_address', 'src_address_adj', 'reg_address_adj',
                                           'Manual_Match_N', 'Manual_Match_NA', 'srcjoinfields', 'regjoinfields']

    # Need to define proc_num here otherwise will not be carried through as part of 'self'
    proc_num = int
    best_config = int
    conf_file_num = int
    configs = dict
    main_proc = int
    upload_table = str


class UK_Settings:

    runfile_mods = UK_run_files

    df_dtypes = {'Cluster ID': np.float64, 'Confidence Score': np.float, 'ocid': np.str, 'src_name': np.str, 'src_id': np.str,
                 'src_name_adj': np.str, 'src_streetaddress': np.str, 'CH_id': np.str, 'CH_name_adj': np.str,
                 'CH_address': np.str, 'src_name_short': np.str, 'CH_name_short': np.str, 'leven_dist_N': np.int,
                 }

    training_cols = ['src_name', 'CH_name', 'Manual_Match_N', 'CH_id', 'CH_address', 'leven_dist_N']

    manual_matches_cols = ['src_id','src_name', 'CH_name', 'Manual_Match_N',
                           # 'src_streetaddress',
                           'CH_address', 'CH_id', 'src_name_short', 'CH_name_short', 'leven_dist_N',
                           # 'ocid'
                           ]

    stats_cols = ['Config_File', 'Total_Matches', 'Percent_Matches', 'Optim_Matches', 'Percent_Precision',
                  'Percent_Recall', 'Leven_Dist_Avg']

    dbUpload_cols = ['src_name', 'src_id', 'CH_name', 'Manual_Match_N', 'company_url', 'CH_id', 'CH_address', 'leven_dist_N']

    # Need to define proc_num here otherwise will not be carried through as part of 'self'
    proc_num = int
    conf_file_num = int
    configs = dict
    main_proc = int
    upload_table = str
    transfer_table = str
    best_config = int

    directories = directories.dirs["dirs"]

    # Needed to kick start the setup dirs function as proc_type usually
    # defined after the directories are setup (which doesnt make sense - fix this)
    proc_type = 'Name_Only'


class UK_entities(UK_Settings):

    def __init__(self):
        super().__init__(self)

    df_dtypes = {'Cluster ID': np.int, 'Confidence Score': np.float,
                 'src_name': np.str, 'src_name_adj': np.str, 'src_streetaddress': np.str, 'src_streetaddress_adj': np.str, 'src_name_short': np.str,
                 'src_address_locality': np.str, 'src_address_postalcode': np.str, 'src_address_streetaddress': np.str,
                 'reg_id': np.str, 'reg_name_adj': np.str, 'reg_address': np.str, 'reg_address_adj': np.str,  'reg_name_short': np.str,
                 'leven_dist_N': np.int, 'leven_dist_NA': np.int,
                 'Manual_Match_N':np.str,  'Manual_Match_NA': np.str,
                 'src_joinfields':np.str, 'reg_joinfields': np.str, 'src_tag':np.str,
                 'reg_source': np.str, 'reg_created_at': np.str, 'reg_scheme':np.str, 'src_address_adj': np.str,
                 'match_by': np.str, 'match_date':np.str
                 }

    runfile_mods = UK_ent_run_files

    # reg_data_source = 'uk_data.entity'
    reg_data_source = 'ocds.orgs_ocds'
    src_data_source = 'ocds.ocds_tenders_view'

    dedupe_cols = ['src_name','src_tag', 'src_name_adj', 'src_address_adj',
                   'reg_id', 'reg_name', 'reg_name_adj', 'reg_address', 'reg_address_adj','src_joinfields', 'reg_joinfields',
                   'reg_source', 'reg_created_at', 'reg_scheme']

    raw_src_data_cols = ['src_name','src_tag','src_address_locality','src_address_postalcode', 'src_address_countryname','src_address_streetaddress']

    dbUpload_cols = ['src_name', 'reg_name','leven_dist_N',  'Manual_Match_N', 'src_address_adj', 'reg_address_adj', 'Manual_Match_NA', 'leven_dist_NA', 'reg_id',  'src_tag', 'src_id', 'reg_source', 'reg_created_at', 'reg_scheme','match_date', 'match_by']

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
                            'reg_created_at',
                            'reg_scheme',
                            'match_by',
                            'match_date'
                            ]


class CQC_settings(UK_entities):

    def __init__(self):
        super().__init__(self)

    runfile_mods = CQC_run_files

    src_data_source = None
    reg_data_source = 'uk_data.cqc_hsca_locations'
    # ocds.cqc_orgs_lookup, ocds.cqc_ratings_locations, ocds.cqc_ratings_providers

    df_dtypes = {'Cluster ID': np.int, 'Confidence Score': np.float,
                 'src_name': np.str, 'src_name_adj': np.str,
                 'src_name_short': np.str,
                 'reg_id': np.str, 'reg_name_adj': np.str,
                 'reg_name_short': np.str,
                 'leven_dist_N': np.int, 'leven_dist_NA': np.int,
                 'Manual_Match_N': np.str, 'Manual_Match_NA': np.str,
                 'src_str_len' : np.int, 'reg_str_len': np.int,
                 'match_by': np.str, 'match_date': np.str

                 }

    raw_src_data_cols = ['supplier_source_string','sum','count']

    dbUpload_cols = ['src_name', 'reg_name', 'src_str_len','reg_str_len', 'leven_dist_N', 'Manual_Match_N', 'reg_id','src_amount','src_count','match_date', 'match_by']

