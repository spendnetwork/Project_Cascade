import pandas as pd
import os
import subprocess
from shutil import copyfile
import pdb
from runfile import Main, logging
from datetime import datetime
from csvdedupe.csvlink import launch_new_instance as launch_matching
from csvdedupe.csvdedupe import launch_new_instance as launch_clustering
import sys
import glob

class Matching(Main):
    def __init__(self, settings, src_df, reg_df):
        super().__init__(settings)
        self.src_df = src_df
        self.reg_df = reg_df
        self.src_fields = None
        self.reg_fields = None
        self.train = None

    def dedupe(self):

        # Run dedupe for matching and clustering

        self.src_fields = self.configs['processes'][self.proc_type][1]['dedupe_field_names']['source_data']
        self.reg_fields = self.configs['processes'][self.proc_type][1]['dedupe_field_names']['registry_data']
        self.train = ['--skip_training' if self.in_args.training else '']
        # if not os.path.exists(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)):
        if not os.path.exists(self.directories["match_output_file"].format(self.region_dir, self.proc_type)):

            if not self.in_args.split:
                self.dedupeMatch()
            else:
                self.dedupeSplitMatch()
                # Unmatched source strings are normally kept (in in_args.split = False), and then clustered
                # Because of file sizes of split files, these are removed. Below adds them back in
                self.addBackUnmatchedSrcStrings()

        if not os.path.exists(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)):
            self.dedupeCluster()

        if not os.path.exists(self.directories['assigned_output_file'].format(self.region_dir, self.proc_type)):
            clust_df = pd.read_csv(self.directories["cluster_output_file"].format(self.region_dir, self.proc_type), index_col=None,
                                   dtype=self.df_dtypes)

            # Copy registry data to high-confidence cluster records
            clust_df = self.data_processing.AssignRegDataToClusters(clust_df, self.directories[
                'assigned_output_file'].format(self.region_dir, self.proc_type)).assign()

            clust_df = clust_df.fillna(value="")

            # Add string length columns
            clust_df['src_str_len'] = clust_df['src_name'].str.len().astype(int)
            clust_df['reg_str_len'] = clust_df['reg_name'].str.len().astype(int)

            # Adds leven_dist column and verify matches based on config process criteria:
            clust_df = self.data_processing.LevDist(self, clust_df, self.directories["assigned_output_file"].format(
                                                                     self.region_dir,
                                                                     self.proc_type)).addLevDist()
            clust_df = clust_df[pd.notnull(clust_df['src_name'])]
        else:
            # clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type),index_col=None, dtype=self.df_dtypes)
            clust_df = pd.read_csv(self.directories["assigned_output_file"].format(self.region_dir, self.proc_type),
                                   dtype=self.df_dtypes)
            clust_df = clust_df[pd.notnull(clust_df['src_name'])]

        return clust_df

    def dedupeSplitMatch(self):
        '''
        Takes all files in Adj_Data/Splits and iteratively runs them through matching process before combining into one
        output file
        :return:
        '''
        reg_df = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
            self.in_args.reg_adj)
        files = glob.glob(os.path.join(self.directories["splits_inputs_dir"].format(self.region_dir, self.proc_type),'*'))
        numfiles = len(files)
        fileno = 0

        for src_file in files:
            fileno += 1

            # Matching:
            if not os.path.exists(self.directories['match_output_file'].format(self.region_dir, self.proc_type)):
                logging.info("Starting matching of split file " + str(fileno) + '/' + str(numfiles))

                cmd = ['csvlink '
                       + str(src_file) + ' '
                       + str(reg_df)
                       + ' --field_names_1 ' + ' '.join(self.src_fields)
                       + ' --field_names_2 ' + ' '.join(self.reg_fields)
                       + ' --training_file ' + self.directories['manual_training_file'].format(self.region_dir,
                                                                                               self.proc_type)
                       + ' --settings_file ' + self.directories['learned_settings_file'].format(self.region_dir,
                                                                                                self.proc_type)
                       + ' --output_file ' + os.path.join(self.directories['splits_outputs_dir'].format(self.region_dir, self.proc_type), str(fileno) + '.csv') + ' '
                       + str(self.train[0])
                       + ' --inner_join'
                       ]

                p = subprocess.Popen(cmd, shell=True)
                p.wait()

        # Join files together to create one matched output file
        files = glob.glob(os.path.join(self.directories["splits_outputs_dir"].format(self.region_dir, self.proc_type), '*'))
        frames = []
        # Load in each file and add to frames list
        for file in files:
            df = pd.read_csv(file)
            frames.append(df)
        # concatenate list into one df
        df = pd.concat(frames)
        df = df.drop_duplicates()

        # Save as normal to match outputs folder
        df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)

    def addBackUnmatchedSrcStrings(self):
        '''
        When the files are split into small chunks, to avoid introducing i.e. 40k unmatched rows * number of split files
        the unmatched rows are removed. This function serves to add those unmatched source strings back in, once, at the
        end of the matching of the split files. This then allows clustering and subsequent increase in number of matches
        '''

        matchdf = pd.read_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type))

        srcdf = pd.read_csv(self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj))

        mergedf = pd.merge(matchdf, srcdf, how='right', on=['src_name','src_amount','src_count','src_name_adj'])

        mergedf.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)


    def dedupeMatch(self):
        """
    	Deduping - first the registry and source data are matched using dedupes csvlink,
    	then the matched file is put into clusters
        :param reg_df:
        :param src_file:
    	:param directories: file/folder locations
    	:param  config_files: the main config files
    	:param proc_type: the 'type' of the process (Name, Name & Address)
    	:param proc_num: the individual process within the config file
    	:return None
    	:output : matched output file
    	:output : matched and clustered output file
    	"""

        src_file = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj)

        reg_df = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
            self.in_args.reg_adj)

        # Matching:
        if not os.path.exists(self.directories['match_output_file'].format(self.region_dir, self.proc_type)):
            if self.in_args.recycle:

                # Copy manual matching file over to build on for clustering
                copyfile(self.directories['manual_matching_train_backup'].format(self.region_dir),
                         self.directories['manual_training_file'].format(self.region_dir, self.proc_type))

            logging.info("Starting matching...")

            cmd = ['csvlink '
                   + str(src_file) + ' '
                   + str(reg_df)
                   + ' --field_names_1 ' + ' '.join(self.src_fields)
                   + ' --field_names_2 ' + ' '.join(self.reg_fields)
                   + ' --training_file ' + self.directories['manual_training_file'].format(self.region_dir, self.proc_type)
                   + ' --settings_file ' + self.directories['learned_settings_file'].format(self.region_dir,
                                                                                            self.proc_type)
                   + ' --output_file ' + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
                   + str(self.train[0])
                   ]

            p = subprocess.Popen(cmd, shell=True)
            p.wait()

            # sys.argv = [
            #             'csvlink',
            #             str(src_file),
            #             str(reg_df),
            #             '--field_names_1', ' '.join(src_fields), '--field_names_2', ' '.join(reg_fields), '--training_file',
            #             self.directories['manual_training_file'].format(self.region_dir, self.proc_type),
            #             '--sample_size', '500',
            #             '--output_file',
            #             self.directories['match_output_file'].format(self.region_dir, self.proc_type),
            #             str(train[0])
            # ]

            # '--settings_file', os.path.join(os.getcwd(),'learned_settings'),

            # launch_matching()

            df = pd.read_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type),
                             dtype=self.df_dtypes)
            df = df[pd.notnull(df['src_name'])]
            df = df.drop_duplicates()
            df.to_csv(self.directories['match_output_file'].format(self.region_dir, self.proc_type), index=False)


    def dedupeCluster(self):
        # Clustering:
        if not os.path.exists(self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)):
            # Copy training file from first clustering session if recycle mode
            if self.in_args.recycle:
                copyfile(self.directories['cluster_training_backup'].format(self.region_dir),
                         self.directories['cluster_training_file'].format(self.region_dir, self.proc_type))

            logging.info("Starting clustering...")

            cmd = ['csvdedupe '
                   + self.directories['match_output_file'].format(self.region_dir, self.proc_type) + ' '
                   + ' --field_names ' + ' '.join(self.src_fields) + ' '
                   + str(self.train[0])
                   + ' --training_file ' + self.directories['cluster_training_file'].format(self.region_dir,self.proc_type)
                   + ' --settings_file ' + self.directories['learned_settings_file'].format(self.region_dir,
                                                                                            self.proc_type)
                   + ' --output_file ' + self.directories['cluster_output_file'].format(self.region_dir,self.proc_type)]

            p = subprocess.Popen(cmd, shell=True)
            p.wait()  # wait for subprocess to finish

            # sys.argv = [
            #             'csvdedupe',
            #             self.directories['match_output_file'].format(self.region_dir, self.proc_type),
            #             '--field_names',
            #             ' '.join(src_fields),
            #             str(train[0]),
            #             '--training_file',
            #             self.directories['cluster_training_file'].format(self.region_dir, self.proc_type),
            #             '--output_file',
            #             self.directories['cluster_output_file'].format(self.region_dir, self.proc_type)
            #             ]
            #
            # launch_clustering()

            if not self.in_args.recycle:
                # Copy training file to backup, so it can be found and copied into recycle phase clustering
                copyfile(self.directories['cluster_training_file'].format(self.region_dir, self.proc_type),
                         self.directories['cluster_training_backup'].format(self.region_dir))
        else:
            pass


