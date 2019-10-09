import sys
import pandas as pd
import os
import subprocess
from shutil import copyfile
from runfile import Main, logging
import glob
from csvdedupe.csvlink import launch_new_instance as launch_matching
from csvdedupe.csvdedupe import launch_new_instance as launch_clustering
import pdb
from dateutil.tz import gettz
import datetime as dt


class Matching(Main):
    def __init__(self, settings):
        super().__init__(settings)
        self.reg_fp = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_reg_data'].format(
            self.in_args.reg_adj)
        self.src_fields = self.configs['processes'][self.proc_type][1]['dedupe_field_names']['source_data']
        self.reg_fields = self.configs['processes'][self.proc_type][1]['dedupe_field_names']['registry_data']
        self.train = ['--skip_training' if self.in_args.training else '']
        self.matched_fp = self.directories["match_output_file"].format(self.region_dir, self.proc_type)
        self.clustered_fp = self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)
        self.assigned_fp = self.directories['assigned_output_file'].format(self.region_dir, self.proc_type)
        self.matches_training_file = self.directories['manual_training_file'].format(self.region_dir, self.proc_type)
        self.learned_settings_file = self.directories['learned_settings_file'].format(self.region_dir, self.proc_type)
        self.cluster_training_file = self.directories['cluster_training_file'].format(self.region_dir, self.proc_type)

    def dedupe(self):
        """
        Main function for the class.
        Takes in the two source and registry dataframes and using training data will join right onto left and return matches.
        Matched rows are then clustered together into groups.
        Levenshtein distances between each match are then added.
        Within each cluster, unmatched rows are assigned the best match (highest levenshtein score) in that cluster

        Order = match -> cluster -> assign

        :return: clust_df - the matched, clustered and 'assigned' dataframe
        """
        # MATCHING

        # If the matches file doesn't exist
        if not os.path.exists(self.matched_fp):

            # For larger files, split arg can be used to break the file into several smaller csvs to then run multiple
            # consecutive matching/clustering sessions
            if not self.in_args.split:

                self.dedupeMatch()

            else:
                self.dedupeSplitMatch()

                # Unmatched source strings are normally kept (when in_args.split = False), and then clustered
                # Because of file sizes of split files, these are removed before concatenating. Below adds them back in
                self.addBackUnmatchedSrcStrings()

        # CLUSTERING

        # If the clustered file doesn't exist
        if not os.path.exists(self.clustered_fp):
            self.dedupeCluster()

        # ASSIGNING MATCHES

        # If the 'assigned' file doesn't exist
        if not os.path.exists(self.assigned_fp):
            clust_df = pd.read_csv(self.clustered_fp, index_col=None, dtype=self.df_dtypes)

            # Assign matches within clusters
            clust_df = self.data_processing.AssignRegDataToClusters().assign(clust_df, self.assigned_fp)

            # Convert NaNs to empty strings
            clust_df = clust_df.fillna(value="")

            # Add string length columns
            clust_df['src_str_len'] = clust_df['src_name'].str.len().astype(int)
            clust_df['reg_str_len'] = clust_df['reg_name'].str.len().astype(int)

            # Add match date column
            # clust_df['match_date'] = pd.to_datetime('today')
            clust_df['match_date'] = dt.datetime.now(gettz()).isoformat()

            # Add blank match_by column
            clust_df['match_by'] = ' '
            clust_df.reg_source = clust_df.reg_source.replace(r'^\s*$', '.', regex=True)
            clust_df.reg_scheme = clust_df.reg_scheme.replace(r'^\s*$', '.', regex=True)

            # Add levenshtein distance to measure the quality of the matches
            clust_df = self.data_processing.LevDist(self, clust_df, self.assigned_fp).addLevDist()

            # Filter rows for non-blank source name
            clust_df = clust_df[pd.notnull(clust_df['src_name'])]

            clust_df.to_csv(self.clustered_fp, index=False)

        # else:
        #     # If the assigned file does exist, load it into memory to be returned
        #     clust_df = pd.read_csv(self.assigned_fp,
        #                            dtype=self.df_dtypes)

        # # Filter rows for non-blank source name
        # clust_df = clust_df[pd.notnull(clust_df['src_name'])]

        # return clust_df

    def dedupeSplitMatch(self):
        '''
        Takes all files in Adj_Data/Splits and iteratively runs them through matching process before combining into one
        output file
        :return:
        '''

        # Define filepaths and vars to be passed to dedupe
        splits_input_dir = self.directories["splits_inputs_dir"].format(self.region_dir, self.proc_type)
        splits_output_dir = self.directories['splits_outputs_dir'].format(self.region_dir, self.proc_type)
        files = glob.glob(os.path.join(splits_input_dir ,'*'))
        numfiles = len(files)
        fileno = 0

        for src_fp in files:
            fileno += 1

            if not os.path.exists(self.matched_fp):
                logging.info(f"Starting matching of split file {str(fileno)} / {str(numfiles)}")

                sys.argv = [
                    'csvlink',
                    str(src_fp),
                    str(self.reg_fp),
                    '--field_names_1',
                        ' '.join(self.src_fields),
                    '--field_names_2',
                        ' '.join(self.reg_fields),
                    '--training_file',
                        self.matches_training_file,
                    '--settings_file',
                        self.learned_settings_file,
                    '--output_file',
                        os.path.join(splits_output_dir, str(fileno) + '.csv'),
                    str(self.train[0])
                ]

                launch_matching()

        files = glob.glob(os.path.join(splits_output_dir, '*'))
        frames = []
        # Load in each file and add to frames list
        for file in files:
            df = pd.read_csv(file)
            frames.append(df)
        # Concatenate files in list into one df
        df = pd.concat(frames)
        df = df.drop_duplicates()

        # Save as normal to match outputs folder
        df.to_csv(self.matched_fp, index=False)

    def addBackUnmatchedSrcStrings(self):
        '''
        When the files are split into small chunks, to avoid introducing i.e. 40k unmatched rows * number of split files
        the unmatched rows are removed. This function serves to add those unmatched source strings back in, once, at the
        end of the matching of the split files. This then allows clustering and subsequent increase in number of matches
        '''

        matchdf = pd.read_csv(self.matched_fp)

        srcdf = pd.read_csv(self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj))

        mergedf = pd.merge(matchdf, srcdf, how='right', on=['src_name','src_tag','src_name_adj','src_name_adj','src_address_adj', 'src_joinfields'])

        mergedf.to_csv(self.matched_fp, index=False)

    def dedupeMatch(self):
        """
    	Deduping - first the registry and source data are matched using dedupes csvlink,
    	then the matched file is put into clusters

        :param src_file:
    	:param directories: file/folder locations
    	:param  config_files: the main config files
    	:param proc_type: the 'type' of the process (Name, Name & Address)
    	:param proc_num: the individual process within the config file
    	:return None
    	:output : matched output file
    	:output : matched and clustered output file
    	"""

        src_fp = self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj)

        # Matching:
        if not os.path.exists(self.matched_fp):
            if self.in_args.recycle:
                # Copy manual matching file over to build on for clustering
                copyfile(self.directories['manual_matching_train_backup'].format(self.region_dir),
                         self.matches_training_file)

            logging.info("Starting matching...")

            sys.argv = [
                'csvlink',
                str(src_fp),
                str(self.reg_fp),
                '--field_names_1',
                    ' '.join(self.src_fields),
                '--field_names_2',
                    ' '.join(self.reg_fields),
                '--training_file',
                    self.matches_training_file,
                '--settings_file',
                    self.learned_settings_file,
                '--output_file',
                    self.matched_fp,
                str(self.train[0])
            ]

            launch_matching()

            df = pd.read_csv(self.matched_fp,dtype=self.df_dtypes)
            df = df[pd.notnull(df['src_name'])]
            df = df.drop_duplicates()
            df.to_csv(self.matched_fp, index=False)

    def dedupeCluster(self):
        if not os.path.exists(self.clustered_fp):
            # Copy training file from first clustering session if recycle mode
            if self.in_args.recycle:
                copyfile(self.directories['cluster_training_backup'].format(self.region_dir),
                         self.cluster_training_file)

            logging.info("Starting clustering...")

            sys.argv = [
                'csvdedupe',
                self.matched_fp,
                '--field_names',
                    ' '.join(self.src_fields),
                str(self.train[0]),
                '--training_file',
                    self.cluster_training_file,
                '--settings_file',
                    self.learned_settings_file,
                '--output_file',
                    self.clustered_fp
            ]

            launch_clustering()

            if not self.in_args.recycle:
                # Copy training file to backup, so it can be found and copied into recycle phase clustering
                copyfile(self.cluster_training_file, self.directories['cluster_training_backup'].format(self.region_dir))
        else:
            pass
