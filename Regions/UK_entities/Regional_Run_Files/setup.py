import pdb
import os
import shutil
import glob


class Setup:
    def __init__(self, settings):
        self.directories = settings.directories
        self.region_dir = settings.region_dir
        self.proc_type = settings.proc_type
        self.in_args = settings.in_args

    def SetupDirs(self):
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


class ClearFiles(Setup):
    # def _init__(self, settings):
    #     super().__init__(settings)

    def clearFiles(self):

        if self.in_args.clear_all:
            self.clear_manual_matches()
            self.clear_extract_matches()
            self.clear_deduped_data()
            self.clear_raw_data()
            self.clear_adj_data()

        if self.in_args.clear_adj:
            self.clear_adj_data()
            self.clear_deduped_data()
            self.clear_manual_matches()
            self.clear_extract_matches()

        if self.in_args.clear_outputs:
            self.clear_manual_matches()
            self.clear_extract_matches()
            self.clear_deduped_data()

        if self.in_args.clear_post_matching:
            self.clear_extract_matches()
            self.clear_manual_matches()

    @staticmethod
    def removeFiles(fp):
        files = glob.glob(fp)
        for file in files:
            os.remove(file)

    def clear_manual_matches(self):
        self.removeFiles(os.path.join(self.directories['confirmed_matches_dir'].format(self.region_dir, self.proc_type), '*'))

    def clear_extract_matches(self):
        self.removeFiles(os.path.join(self.directories['proc_type_matches_dir'].format(self.region_dir, self.proc_type), '*'))

    def clear_deduped_data(self):
        self.removeFiles(os.path.join(self.directories['deduped_dir'].format(self.region_dir, self.proc_type), '*'))

    def clear_raw_data(self):
        self.removeFiles(os.path.join(self.directories['raw_dir'].format(self.region_dir), '*'))

    def clear_adj_data(self):
        self.removeFiles(os.path.join(self.directories['adj_dir'].format(self.region_dir), '*'))

