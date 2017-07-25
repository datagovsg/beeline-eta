import boto
import boto.s3.connection
import os
from boto.s3.key import Key

AWS_ACCESS_KEY_ID = os.environ.get("BUCKETEER_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("BUCKETEER_AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.environ.get("BUCKETEER_BUCKET_NAME")

def get_connection_and_bucket():
    conn = boto.s3.connect_to_region(
               'us-east-1',
               aws_access_key_id=AWS_ACCESS_KEY_ID,
               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
               is_secure=True,
               calling_format = boto.s3.connection.OrdinaryCallingFormat(),
           )
    bucket = conn.get_bucket(BUCKET_NAME)
    return conn, bucket

def split_filename(full_filename):
    if '/' not in full_filename:
        return full_filename
    filename_parts = full_filename.split('/')
    path, filename = '/'.join(filename_parts[:-1]), filename_parts[-1]
    return path, filename

def upload_file(full_filename):
    path, filename = split_filename(full_filename)
    conn, bucket = get_connection_and_bucket()
    full_key_name = os.path.join(path, filename)
    print('Attempting to upload to {}'.format(full_key_name))
    key = bucket.new_key(full_key_name)
    key.set_contents_from_filename(full_key_name)

def download_file(full_filename):
    path, filename = split_filename(full_filename)
    conn, bucket = get_connection_and_bucket()
    full_key_name = os.path.join(path, filename)
    print('Attempting to download from {}'.format(full_key_name))
    k = Key(bucket)
    k.key = full_key_name
    k.get_contents_to_filename(filename)

def destroy_predictions():
    conn, bucket = get_connection_and_bucket()
    for key in bucket:
        key.delete()

def list_files(path):
    conn, bucket = get_connection_and_bucket()
    return [key.name for key in bucket]
