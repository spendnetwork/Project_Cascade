from dotenv import load_dotenv, find_dotenv
import os
from runfile import Main, logging
import glob
import boto3
from botocore.exceptions import ClientError
import pdb
from zipfile import ZipFile

# get the remote database details from .env
load_dotenv(find_dotenv())
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")


class AwsTransfers(Main):
    '''
    Class contains functions to download and upload matches from/to AWS S3 bucket.
    '''

    def __init__(self, settings):
        super().__init__(settings)
        self.bucket = 'org-matching'

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

        # Download verified matches from s3 bucket if prodn_verified argument (production only)
        if self.in_args.prodn_verified:
            self.process_verified_files()

    def process_verified_files(self):
        """
        Establishes connection to S3 bucket using boto3 library and credentials found in .env
        Takes verified matches files placed in /verified_matches in S3 bucket and uploads them to the db table
        Files are then archived in S3 bucket as zip files, and then removed from S3 bucket/verified

        """
        # Scan s3 verified folder for files
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Verified_Matches/')

        # Ignore first file entry in dict as is just the folder name. Returns a list of files
        files = response['Contents'][1:]

        # For any files in /s3/verified/ - download them to local /verified_matches/
        for i in range(len(files)):

            s3.download_file(self.bucket,
                             files[i]['Key'],
                             os.path.join(self.directories['verified_matches_dir'].format(self.region_dir,
                                                                                 self.proc_type),
                                          os.path.basename(files[i]['Key'])))

        # Upload all files in verified_matches_dir to our database:
        self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

        for i in range(len(files)):
            try:
                # Delete from unverified folder (if hasn't been done by team already) so team know which haven't been
                # verified yet (located via date prefix of verified file incase of name change by team)
                response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Unverified_Matches/' + os.path.basename(files[i]['Key'])[:10])
                file = response['Contents'][:]
                s3.delete_object(Bucket=self.bucket, Key=file[i]['Key'])
            except:
                pass

            # Convert file to zip file for archiving
            zip_fp = os.path.join(self.directories['verified_matches_dir'].format(self.region_dir, self.proc_type)
                                        , os.path.basename(files[i]['Key'][:-4] + '.zip'))

            with ZipFile(zip_fp, 'w') as myzip:
                myzip.write(zip_fp)

            # Upload zip file to S3 Archive
            self.upload_file(zip_fp, self.bucket, 'UK_entities/Archive/' + os.path.basename(zip_fp))

            # Delete matches csv from s3 verified folder
            s3.delete_object(Bucket=self.bucket, Key=files[i]['Key'])


    @staticmethod
    def upload_file(file_name, bucket, object_name=None):
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
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.exception(e)
            return False
        logging.info("Upload to S3 bucket complete!")
        return True