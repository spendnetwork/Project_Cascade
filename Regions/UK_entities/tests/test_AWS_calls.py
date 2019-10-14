from ..Regional_Run_Files import AWS_calls
import pdb
from dotenv import load_dotenv, find_dotenv
import os
import boto3

# get the remote database details from .env
load_dotenv(find_dotenv())
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")


def test_processverifiedfiles():
    pass

def test_upload(settings, adjust_default_args):

    upload_file = os.path.join(settings.testdir, 'test_data', adjust_default_args.reg_adj)
    bucket = 'org-matching'

    # Upload file
    AWS_calls.AwsTransfers.upload_file(upload_file, bucket, 'UK_entities/Tests/reg_data_adj_test.csv')

    # Check file exists in S3 folder
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = s3.list_objects(Bucket=bucket, Prefix='UK_entities/Tests/')
    file = response['Contents'][:]
    assert file[0]['Key'] == 'UK_entities/Tests/reg_data_adj_test.csv'

    # Delete file from s3
    s3.delete_object(Bucket=bucket, Key=file[0]['Key'])

def test_zipfilecreated():
    pass