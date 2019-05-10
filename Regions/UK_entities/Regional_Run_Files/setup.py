import pdb
import os


class Setup:
    def __init__(self, settings):
        self.directories = settings.directories
        self.region_dir = settings.region_dir
        self.proc_type = settings.proc_type

    def setupDirs(self):
        '''
        Establishes the directories required as specified by the config dirs file
        :param directories: filepath dictionary
        :param region_dir: root folder
        :param proc_type: process type i.e. Name_Only, Name_Address
        :return: None
        '''

        if not os.path.exists(self.directories['proc_type_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['proc_type_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['confirmed_matches_dir'].format(self.region_dir, self.proc_type)) :
            os.makedirs(self.directories['confirmed_matches_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['deduped_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['deduped_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['proc_type_train_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['proc_type_train_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['proc_type_train_clust_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['proc_type_train_clust_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['proc_type_train_match_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['proc_type_train_match_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['proc_type_matches_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['proc_type_matches_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(self.directories['backups_dir'].format(self.region_dir, self.proc_type)):
            os.makedirs(self.directories['backups_dir'].format(self.region_dir, self.proc_type))
        if not os.path.exists(os.path.join(self.region_dir, 'Config_Files')):
            os.makedirs(os.path.join(self.region_dir, 'Config_Files'))

    def setupRawDirs(self):
        if not os.path.exists(self.directories['raw_dir'].format(self.region_dir)):
            os.makedirs(self.directories['raw_dir'].format(self.region_dir))
        if not os.path.exists(self.directories['adj_dir'].format(self.region_dir)):
            os.makedirs(self.directories['adj_dir'].format(self.region_dir))