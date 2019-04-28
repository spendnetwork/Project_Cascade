import pdb
import os

def setupDirs(directories, regiondir, proc_type='Name_Only'):
    '''
    Establishes the directories required as specified by the config dirs file
    :param directories: filepath dictionary
    :param regiondir: root folder
    :param proc_type: process type i.e. Name_Only, Name_Address
    :return: None
    '''

    if not os.path.exists(directories['proc_type_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['proc_type_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['confirmed_matches_dir'].format(regiondir, proc_type)) :
        os.makedirs(directories['confirmed_matches_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['deduped_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['deduped_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['proc_type_train_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['proc_type_train_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['proc_type_train_clust_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['proc_type_train_clust_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['proc_type_train_match_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['proc_type_train_match_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['proc_type_matches_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['proc_type_matches_dir'].format(regiondir, proc_type))
    if not os.path.exists(directories['backups_dir'].format(regiondir, proc_type)):
        os.makedirs(directories['backups_dir'].format(regiondir, proc_type))
    if not os.path.exists(os.path.join(regiondir, 'Config_Files')):
        os.makedirs(os.path.join(regiondir, 'Config_Files'))

def setupRawDirs(regiondir, directories):
    if not os.path.exists(directories['raw_dir'].format(regiondir)):
        os.makedirs(directories['raw_dir'].format(regiondir))
    if not os.path.exists(directories['adj_dir'].format(regiondir)):
        os.makedirs(directories['adj_dir'].format(regiondir))