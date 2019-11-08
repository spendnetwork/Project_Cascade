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
        self.mtrain = ['--skip_training' if not self.in_args.mtraining else '']
        self.ctrain = ['--skip_training' if not self.in_args.ctraining else '']
        self.matched_fp = self.directories["match_output_file"].format(self.region_dir, self.proc_type)
        self.clustered_fp = self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)
        self.manual_clustered_fp = self.directories["mancluster_output_file"].format(self.region_dir, self.proc_type)
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

        if not os.path.exists(self.manual_clustered_fp):
            self.manualclustering()

        # ASSIGNING MATCHES

        # If the 'assigned' file doesn't exist, take each cluster and stretch out the in-cluster matches
        if not os.path.exists(self.assigned_fp):
            self.assignmatcheswithinclusters()

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
                    str(self.mtrain[0])
                ]

                if self.mtrain[0] == '':
                    sys.argv = sys.argv[:-1]

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
                str(self.mtrain[0])
            ]
            if self.mtrain[0] == '':
                sys.argv = sys.argv[:-1]

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
                '--training_file',
                    self.cluster_training_file,
                '--settings_file',
                    self.learned_settings_file,
                '--output_file',
                    self.clustered_fp,
                str(self.ctrain[0])
            ]
            if self.ctrain[0] == '':
                sys.argv = sys.argv[:-1]
            launch_clustering()

            if not self.in_args.recycle:
                # Copy training file to backup, so it can be found and copied into recycle phase clustering
                copyfile(self.cluster_training_file, self.directories['cluster_training_backup'].format(self.region_dir))
        else:
            pass


    def manualclustering(self):

        df = pd.read_csv(self.clustered_fp)

        # Rename columns so can be picked up by SQL COPY function
        df = df.rename(columns={'Cluster ID': 'Cluster_ID', 'Confidence Score': 'Confidence_Score'})

        df['Cluster_ID'] = df['Cluster_ID'].astype(float)
        max_clust = df['Cluster_ID'].max() + 1
        # Get count of cluster_ids to see which rows are/aren't in a cluster
        # Identify the highest Cluster_ID. This is where we start the additional grouping
        # Use groupby on string adj to group identical strgs. We could either try to add strings to larger clusters, or just segregate it entirely and look only at non clustered strings.
        # Going to look at just segregated ones for now i.e. not trying add to dedupe-clusters.

        # Get unique Cluster_ID's. Split df into two - >1 x id and 1 x id.

        # Below line is wrong - if there's only 1 src_name but belongs to a cluster of similar src_names, will be split out and included in 'nonclust'
        # counts = df.groupby(['Cluster_ID','src_name_adj']).size()

        counts = df.groupby(['Cluster_ID']).size()
        new_df = counts.to_frame(name = 'size').reset_index()

        # Do join for clust back to original df
        clust = new_df[new_df['size'] > 1]
        # clust = clust.join(df, on='Cluster_ID', how='right', lsuffix='_multipleclust')
        clust = clust.merge(df, on='Cluster_ID', how='left')
        clust = clust.drop(columns=['size']).sort_values('src_name_adj').reset_index(drop=True)

        # Do left join for nonclust back to original df
        nonclust = new_df[new_df['size']==1]
        nonclust = nonclust.merge(df, on='Cluster_ID', how='left')
        nonclust = nonclust.drop(columns=['size']).sort_values('src_name_adj').reset_index(drop=True)

        # Now have two dataframes, one with actual clusters (clust) and one with Cluster_IDs but containing only one string (nonclust)
        # ngroup() is used to assign an increasing index number to each group i.e. a new Cluster_ID
        nonclust['Group ID'] = nonclust.groupby(['src_name_adj']).ngroup()

        # now need to set this group id to not overlap with old Cluster_IDs
        nonclust['Group ID'] += max_clust
        nonclust['Cluster_ID'] = nonclust['Group ID']
        nonclust.drop(columns=['Group ID'], inplace=True)
        df = pd.concat([clust, nonclust], sort=True)
        df.to_csv(self.manual_clustered_fp, index=False)

    def assignmatcheswithinclusters(self):

        clust_df = pd.read_csv(self.manual_clustered_fp, index_col=None, dtype=self.df_dtypes)

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
        clust_df.match_source = clust_df.match_source.replace(r'^\s*$', '.', regex=True)
        clust_df.reg_scheme = clust_df.reg_scheme.replace(r'^\s*$', '.', regex=True)

        # Add levenshtein distance to measure the quality of the matches
        clust_df = self.data_processing.LevDist(self, clust_df, self.assigned_fp).addLevDist()

        # Filter rows for non-blank source name
        clust_df = clust_df[pd.notnull(clust_df['src_name'])]

        clust_df.to_csv(self.assigned_fp, index=False)

