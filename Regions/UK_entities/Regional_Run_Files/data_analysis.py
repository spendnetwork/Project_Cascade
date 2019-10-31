import pandas as pd
import os
import numpy as np
import pdb
from runfile import Main, logging


class StatsCalculations(Main):
    """
	For each process outlined in the config file, after each process is completed
	verify the matches that meet the match % criteria into a new file
	extractions based on the different leven ratio values

	:return : None
	:output : a short stats file for each config file for manual comparison to see which is better
	"""
    def __init__(self, settings):
        super().__init__(settings)
        self.clustered_fp = self.directories["cluster_output_file"].format(self.region_dir, self.proc_type)
        self.filtered_matches = self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' \
                                + str(self.conf_file_num) + '.csv'

    def calculate_internals(self):

        # Remove old stats file if exists and if first iteration over config files:
        if os.path.exists(self.directories['stats_file'].format(self.region_dir, self.proc_type)):
            if self.conf_file_num == 1:
                os.remove(self.directories['stats_file'].format(self.region_dir, self.proc_type))

        srcdf = pd.read_csv(self.directories['adj_dir'].format(self.region_dir) + self.directories['adj_src_data'].format(
            self.in_args.src_adj))
        clustdf = pd.read_csv(self.clustered_fp, index_col=None, dtype=self.df_dtypes)
        filterdf = pd.read_csv(self.filtered_matches,  index_col=None, dtype=self.df_dtypes)

        statdf = pd.DataFrame(columns=self.stats_cols)

        statdf.at[self.conf_file_num, 'Total_Src_Rows'] = len(srcdf)
        statdf.at[self.conf_file_num, 'Config_File'] = self.conf_file_num
        statdf.at[self.conf_file_num, 'Original_Matches'] = len(clustdf[pd.notnull(clustdf['reg_name'])])
        statdf.at[self.conf_file_num, 'Pct_Orig_Matches'] = round(len(clustdf[pd.notnull(clustdf['reg_name'])]) / len(srcdf) * 100,2)
        statdf.at[self.conf_file_num, 'Filtered_Matches'] = len(filterdf)
        statdf.at[self.conf_file_num, 'Pct_Filtered_Matches'] = round(len(filterdf) / len(srcdf) * 100,2)

        # Precision - how many of the selected items are relevant to us? (TP/TP+FP)
        # Doesn't look at verified matches, just the filtered matches vs all matches (therefore is manually decided by the
        # config files)
        statdf.at[self.conf_file_num, 'Est_Pct_Precision'] = round(len(filterdf) / len(clustdf) * 100, 2)
        # Recall - how many relevant items have been selected from the entire original source data (TP/TP+FN)
        statdf.at[self.conf_file_num, 'Pct_Recall'] = round(len(filterdf) / len(srcdf) * 100, 2)

        if self.in_args.recycle:
            statdf.at[self.conf_file_num, 'Leven_Dist_Avg'] = round(np.average(filterdf.leven_dist_NA), 2)
        else:
            statdf.at[self.conf_file_num, 'Leven_Dist_Avg'] = round(np.average(filterdf.leven_dist_N), 2)
        # if statsfile doesnt exist, create it
        if not os.path.exists(self.directories['stats_file'].format(self.region_dir, self.proc_type)):
            statdf.to_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type, index=False))
        # if it does exist, concat current results with previous
        else:
            main_stat_file = pd.read_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type), index_col=None, dtype=self.df_dtypes)
            main_stat_file = pd.concat([main_stat_file, statdf], ignore_index=True, sort=True)
            main_stat_file.to_csv(self.directories['stats_file'].format(self.region_dir, self.proc_type), index=False,
                                  columns=self.stats_cols)
            return main_stat_file

    def calculate_externals(self):
        '''
        Calculates final matching statistics by performing analysis over already-matched strings in the database
        Prioritises already-matched strings and then checks the residual to calculate the overall additional effect
        of using dedupe
        '''
        if self.in_args.prodn_unverified:
            conn, cur = self.db_calls.DbCalls.createConnection(self)

            # Truncate assigned matches table ready for new upload
            table = 'matching.assigned_matches'
            print('Clearing matches.assigned_matches table...')

            query = self.db_calls.DbCalls.truncate_table(self, table)
            cur.execute(query)
            conn.commit()

            # Upload new assigned matches to db
            assigned_matches_fp = self.directories["assigned_output_file"].format(self.region_dir, self.proc_type)
            print('Uploading new assigned matches to db...')
            self.db_calls.DbCalls.upload_assigned_matches(self, conn, cur, assigned_matches_fp)
            print('Done.')

            # Perform join of assigned matches file to orgs lookup to filter for strings already matched in orgs_lookup
            # and get counts/stats (outputs script_performance_stats_file to /deduped_data)
            stats_file_fp = self.directories['script_performance_stats_file'].format(self.region_dir, self.proc_type)
            if not os.path.exists(stats_file_fp):
                print('Performing join on matches to orgs_lookup...')
                query = self.db_calls.DbCalls.join_matches_to_orgs_lookup(self)
                df = pd.read_sql(query, con=conn)
                df.to_csv(stats_file_fp, index=False)
                # cur.execute(query)
                # conn.commit()
                conn.close()
                print('Done.')

            # Alter file to add %
            df = pd.read_csv(stats_file_fp)

            bl_df = pd.read_csv(self.directories['blacklisted_string_matches'].format(self.region_dir))
            df['residual_script_matches'] = df['merge_match'] - df['ocds_legalname']
            df['ocds_orgslookup_matches_pct'] = round(df['ocds_legalname'] / df['merge_match'] * 100, 2)
            df['residual_script_matches_pct'] = round(df['residual_script_matches'] / df['merge_match'] * 100,2)
            df['total_matches_pct'] = '' # NEED TO CHECK THIS - WHERE DOES THE NEW STRING COUNT COME FROM?
            df['verified_matches'] = ''
            df['residual_verified_matches_pct'] = ''
            df['blacklisted_matches'] = len(bl_df)
            df.to_csv(stats_file_fp, index=False)





