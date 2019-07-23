from dotenv import load_dotenv, find_dotenv
import os
from runfile import Main
import glob
import boto3
from botocore.exceptions import ClientError


# get the remote database details from .env
load_dotenv(find_dotenv())
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")


class AwsTransfers(Main):

    def __init__(self, settings):
        super().__init__(settings)
        self.bucket = 'org-matching'

    def transfer(self):
        '''
        Core function for the class. Checks args and either uploads to or downloads from s3 bucket
        :return: None
        '''
        # Upload unverified matches to s3 bucket
        if self.in_args.prodn_unverified:
            files = glob.glob(os.path.join(self.directories['unverified_matches_dir'].format(self.region_dir, self.proc_type), '*'))

            for filepath in files:
                filename = os.path.basename(filepath)
                self.upload_file(filepath, self.bucket, 'UK_entities/Unverified_Matches/' + filename)

        # Download verified matches from s3 bucket
        if self.in_args.prodn_verified:
            self.process_verified_files()

    def process_verified_files(self):
        # Scan s3 verified folder for files
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Verified_Matches/')

        # Ignore first file entry in dict as is just the folder name. Returns a list of files
        files = response['Contents'][1:]

        # For any files in /s3/verified/ - transfer them to local /verified_matches/
        for i in range(len(files)):
            s3.download_file(self.bucket,
                             files[i]['Key'],
                             os.path.join(self.directories['verified_matches_dir'].format(self.region_dir,
                                                                                 self.proc_type),
                                          os.path.basename(files[i]['Key'])))

        # Upload all files in verified_matches_dir (downloaded from /verified in s3 bucket:
        self.runfile_mods.db_calls.DbCalls(self).addDataToTable()

        for i in range(len(files)):
            # Delete from s3 verified folder
            s3.delete_object(Bucket=self.bucket, Key=files[i]['Key'])

            try:
                # Delete from unverified folder (if hasn't been done by team already) so team know which haven't been
                # verified yet (located via date prefix of verified file incase of name change by team)
                response = s3.list_objects(Bucket=self.bucket, Prefix='UK_entities/Unverified_Matches/' + os.path.basename(files[i]['Key'])[:10])
                file = response['Contents'][:]
                s3.delete_object(Bucket=self.bucket, Key=file[i]['Key'])
            except:
                pass


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
            print(e)
            return False
        print("Upload to S3 bucket complete!")
        return True