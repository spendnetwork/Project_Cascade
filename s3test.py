import boto3
import pdb
import uuid
from dotenv import load_dotenv, find_dotenv
import os
import logging
from botocore.exceptions import ClientError

# get the remote database details from .env
load_dotenv(find_dotenv())
aws_access_key_id = os.environ.get("aws_access_key_id")
aws_secret_access_key = os.environ.get("aws_secret_access_key")
region = os.environ.get("regn")

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
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':

    s3 = boto3.resource('s3')


    data = open(
        '/Users/davidmellor/Code/Spend_Network/Data_Projects/Project_Cascade/Regions/UK_entities/Outputs/Name_Only/holder/Matches_Buyers_DMadj.csv',
        'rb')
    s3.Bucket('sn-orgmatching').put_object(Key='Matches_Buyers_DMadj.csv', Body=data)

    s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')