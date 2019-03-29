import pdb
import os

def setup_dirs(config_dirs, rootpath, proc_type='Name_Only'):
    '''
    Establishes the directories required as specified by the config dirs file
    :param config_dirs: filepath dictionary
    :param rootpath: root folder
    :param proc_type: process type i.e. Name_Only, Name_Address
    :return: None
    '''
    proc_type_dir = config_dirs['proc_type_dir'].format(rootpath, proc_type)
    if not os.path.exists(proc_type_dir):
        os.makedirs(proc_type_dir)
        os.makedirs(config_dirs['confirmed_matches_dir'].format(rootpath, proc_type))
        os.makedirs(config_dirs['deduped_dir'].format(rootpath, proc_type))

    if not os.path.exists(config_dirs['proc_type_train_dir'].format(rootpath, proc_type)):
        os.makedirs(config_dirs['proc_type_train_dir'].format(rootpath, proc_type))
    if not os.path.exists(config_dirs['proc_type_train_clust_dir'].format(rootpath, proc_type)):
        os.makedirs(config_dirs['proc_type_train_clust_dir'].format(rootpath, proc_type))
    if not os.path.exists(config_dirs['proc_type_train_match_dir'].format(rootpath, proc_type)):
        os.makedirs(config_dirs['proc_type_train_match_dir'].format(rootpath, proc_type))
    if not os.path.exists(config_dirs['proc_type_matches_dir'].format(rootpath, proc_type)):
        os.makedirs(config_dirs['proc_type_matches_dir'].format(rootpath, proc_type))
    if not os.path.exists(config_dirs['backups_dir'].format(rootpath, proc_type)):
        os.makedirs(config_dirs['backups_dir'].format(rootpath, proc_type))
    if not os.path.exists(os.path.join(rootpath, 'Config_Files')):
        os.makedirs(os.path.join(rootpath, 'Config_Files'))

def setupRawDirs(rootpath, config_dirs):
    if not os.path.exists(config_dirs['raw_dir'].format(rootpath)):
        os.makedirs(config_dirs['raw_dir'].format(rootpath))
    if not os.path.exists(config_dirs['adj_dir'].format(rootpath)):
        os.makedirs(config_dirs['adj_dir'].format(rootpath))