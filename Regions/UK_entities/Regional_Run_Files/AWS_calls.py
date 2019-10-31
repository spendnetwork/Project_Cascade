from dotenv import load_dotenv, find_dotenv
import os
from runfile import Main, logging
import glob
import boto3
from botocore.exceptions import ClientError
import pdb
from zipfile import ZipFile
import pandas as pd

# get the remote database details from .env
# load_dotenv(find_dotenv())
# aws_access_key_id = os.environ.get("aws_access_key_id")
# aws_secret_access_key = os.environ.get("aws_secret_access_key")


class AwsTransfers(Main):
    '''
    Class contains functions to download and upload matches from/to AWS S3 bucket.
    '''

    def __init__(self, settings):
        super().__init__(settings)
        self.bucket = 'org-matching'
        self.unverified_file = None

    def transfer(self):
        """
        Core function for the class. Checks args and either uploads to or downloads from s3 bucket
        """

        # Upload unverified matches to s3 bucket if prodn_unverified argument used (production only)
        if self.in_args.prodn_unverified:
            files = glob.glob(os.path.join(self.directories['unverified_matches_dir'].format(self.region_dir, self.proc_type), '*'))

            # Loop through files found in unverified_matches folder
            for filepath in files:
                filename = os.path.basename(filepath)
                # Upload each file to S3 bucket folder
                self.upload_file(filepath, self.bucket, 'UK_entities/Unverified_Matches/' + filename)
                self.unverified_file = filename

            # Zip file creation - note will only work for latest unverified file. Above loop is added just incase
            # any residual files get added manually to S3 bucket.

            # Get filepaths of stats file, filtered and excluded matches files
            stats_fp = self.directories['stats_file'].format(self.region_dir, self.proc_type)
            filtered_matches_fp = self.directories['filtered_matches'].format(self.region_dir, self.proc_type) + '_' + \
                                  str(self.best_config) + '.csv'

            excluded_matches_fp = self.directories['excluded_matches'].format(self.region_dir, self.proc_type) + '_' + \
                                  str(self.best_config) + '.csv'

            blacklisted_strings_fp = self.directories['blacklisted_string_matches'].format(self.region_dir)

            stats_file_fp = self.directories['script_performance_stats_file'].format(self.region_dir, self.proc_type)

            # Assign zip file which will contain above files
            files_zip = self.unverified_file[:10] + "_files.zip"

            with ZipFile(files_zip, 'w') as myzip:
                myzip.write(stats_fp, os.path.basename(stats_fp))
                myzip.write(filtered_matches_fp,os.path.basename(filtered_matches_fp))
                myzip.write(excluded_matches_fp, os.path.basename(excluded_matches_fp))
                myzip.write(blacklisted_strings_fp, os.path.basename(blacklisted_strings_fp))
                myzip.write(stats_file_fp, os.path.basename(stats_file_fp))

            self.upload_file(files_zip, self.bucket, 'UK_entities/Archive/' + files_zip)

        # Download verified matches from s3 bucket if prodn_verified argument (production only)
        if self.in_args.prodn_verified:
            self.process_verified_files()

        # Add confirmed matches/non-matches to training file
        if self.in_args.convert_training:
            self.runfile_mods.convert_training.ConvertToTraining.convert(self)

    def process_verified_files(self):
        """
        Establishes connection to S3 bucket using boto3 library and credentials found in .env
        Takes verified matches files placed in /verified_matches in S3 bucket and uploads them to the db table
        Files are then archived in S3 bucket as zip files, and then removed from S3 bucket/verified
        """

        # Scan s3 verified folder for files
        s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Verified_Matches/')

        # Ignore first file entry in dict as is just the folder name. Returns a list of files
        files = response['Contents'][1:]

        # # For any files in /s3/verified/ - download them to local /verified_matches/
        for i in range(len(files)):
            verified_fp = os.path.join(self.directories['verified_matches_dir'].format(self.region_dir,self.proc_type),os.path.basename(files[i]['Key']))
            s3.download_file(self.bucket,
                             files[i]['Key'],
                             verified_fp)

        # Upload all files in verified_matches_dir to our database:
        if self.in_args.upload:
            self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

        # Loop through retrieved verified matches files from S3 bucket
        for i in range(len(files)):
            try:
                # Delete from unverified folder (if hasn't been done by team already) so team know which haven't been
                # verified yet (located via date prefix of verified file incase of name change by team)
                response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Unverified_Matches/' + os.path.basename(files[i]['Key'])[:10])
                file = response['Contents'][:]
                if self.in_args.upload:
                    s3.delete_object(Bucket=self.bucket, Key=file[i]['Key'])
            except:
                pass

            # For each verified file, iterate over S3 zip files and download the corresponding zip.
            # Need to iterate over both files in /verified and /archive to make sure we aren't adding the wrong information
            # to multiple different files when theres >1 file in these folders.
            s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
            response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Archive/')
            archive_files = response['Contents'][1:]

            # For each file in the archive folder, iterate over the names and find any matches to the date of the current
            # verified file in the iterator
            verified_zip_name = os.path.basename(files[i]['Key'])[:10] + '_files.zip'
            for a in range(len(archive_files)):
                # if they match, open that archive zip file and open the script performance stats file within it
                # and get the length/count of verified matches from the verified file
                archive_file = os.path.basename(archive_files[a]['Key'])
                if archive_file == verified_zip_name:
                    # Download archive file to local verified folder
                    dl_archive_fp = os.path.join(os.path.join(self.directories['verified_matches_dir']
                                                              .format(self.region_dir, self.proc_type), archive_file))
                    s3.download_file(self.bucket,
                                     os.path.join('UK_entities/Archive/', archive_file), dl_archive_fp)

                    # Open archive file
                    with ZipFile(dl_archive_fp, 'r') as z:

                        # Open corresponding verified matches file
                        verified_fp = os.path.join(
                            self.directories['verified_matches_dir'].format(self.region_dir, self.proc_type),
                            os.path.basename(files[i]['Key']))
                        ver_file = pd.read_csv(verified_fp)
                        with z.open('script_performance_stats.csv') as f:
                            # Add additional stats to script performance stats csv
                            stats_file = pd.read_csv(f)
                            true_positives = len(ver_file[ver_file['Manual_Match_N'] == 'Y'])
                            false_positives = len(ver_file[ver_file['Manual_Match_N'] == 'N'])
                            unverified = len(ver_file) - true_positives - false_positives
                            stats_file['true_positives'] = true_positives
                            stats_file['false_positives'] = false_positives
                            stats_file['unverified'] = unverified
                            stats_file.to_csv(self.directories['script_performance_stats_file'].format(self.region_dir, self.proc_type)
                                              ,index=False)

                    stats_file_fp = self.directories['script_performance_stats_file'].format(self.region_dir,
                                                                                             self.proc_type)
                    with ZipFile(dl_archive_fp, 'a') as z:

                        # Add/overwrite new stats file and verified matches file to zip file, then re-upload to S3 /Archive
                        z.write(stats_file_fp, os.path.basename(stats_file_fp))
                        z.write(verified_fp, os.path.basename(verified_fp))
                        self.upload_file(dl_archive_fp, self.bucket, 'UK_entities/Archive/' + archive_file)

            # Delete matches csv from s3 verified folder (if 'upload' arg used)
            if self.in_args.upload:
                s3.delete_object(Bucket=self.bucket, Key=files[i]['Key'])


    def upload_file(self, file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key)
        try:
            s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.exception(e)
            return False
        logging.info("Upload to S3 bucket complete!")

        return True