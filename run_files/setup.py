import os

# Only run through the module once per initiation. Either to get the initial training file
# or to recycle it and get better matches.
def setup_dirs(config_dirs, proc_type, conf_file_num):

        # print("Process type :" + str(proc_type) + "\nConfig file: " + str(conf_file_num))
        # Check if proc_type output directory exists, if not create it and training directories:
        proc_type_dir = config_dirs['proc_type_dir'].format(proc_type)
        if not os.path.exists(proc_type_dir):
            os.makedirs(proc_type_dir)
            os.makedirs(config_dirs['confirmed_matches_dir'].format(proc_type))
            os.makedirs(config_dirs['deduped_dir'].format(proc_type))
        if not os.path.exists(config_dirs['proc_type_train_dir'].format(proc_type)):
            os.makedirs(config_dirs['proc_type_train_dir'].format(proc_type))
        if not os.path.exists(config_dirs['proc_type_train_clust_dir'].format(proc_type)):
            os.makedirs(config_dirs['proc_type_train_clust_dir'].format(proc_type))
        if not os.path.exists(config_dirs['proc_type_train_match_dir'].format(proc_type)):
            os.makedirs(config_dirs['proc_type_train_match_dir'].format(proc_type))
        if not os.path.exists(config_dirs['proc_type_matches_dir'].format(proc_type)):
            os.makedirs(config_dirs['proc_type_matches_dir'].format(proc_type))
        if not os.path.exists(config_dirs['backups_dir'].format(proc_type)):
            os.makedirs(config_dirs['backups_dir'].format(proc_type))