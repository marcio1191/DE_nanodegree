import os
import logging

import boto3
from botocore.exceptions import ClientError


from aws import config


logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class BucketAlreadyExistsError(Exception):
    pass

class S3Connection:
    REGION = config['AWS']['REGION']
    AWS_KEY = config.get('AWS', 'KEY')
    AWS_PASSWORD = config.get('AWS', 'SECRET')
    AWS_SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN')


    def __init__(self):
        self.resource = boto3.resource(
            's3',
            region_name = self.REGION,
            aws_access_key_id = os.getenv('AWS_KEY', self.AWS_KEY),
            aws_secret_access_key= os.getenv('AWS_PASSWORD', self.AWS_PASSWORD),
            aws_session_token=self.AWS_SESSION_TOKEN if self.AWS_SESSION_TOKEN != '' else None,
            )
        self.client = self.resource.meta.client

    def create_bucket(self, name):
        logger.info(f'Creating S3 bucket: "{name}" in region {self.REGION}')
        try:
            self.client.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': self.REGION})
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                raise BucketAlreadyExistsError(e)
            raise e

    def get_bucket(self, bucket_name):
        return self.resource.Bucket(bucket_name)

    def list_buckets(self):
        return self.resource.buckets.all()

    def delete_bucket(self, name, force=False):
        if force:
            self.empty_bucket(name=name)
        bucket = self.get_bucket(name)
        bucket.delete()

    def empty_bucket(self, name):
        bucket = self.get_bucket(name)
        bucket.objects.all().delete()

    def upload_file(self, f_path, bucket, key):
        self.client.upload_file(f_path, Bucket=bucket, Key=key)

    def transverse_all_buckets(self):
        for bucket in self.list_buckets():
            print(bucket.name)


class S3BucketConnection:
    def __init__(self, bucket):
        self.bucket = bucket
    def __repr__(self):
        return f'<S3Bucket({self.bucket})>'

    def __str__(self):
        return self.bucket.__str__()

    def get_all_objects(self):
        return list(self.bucket.objects.all())

    def get_objects_with_prefix(self, prefix):
        # logger.info(f'{self.bucket.name}: ')
        return self.bucket.objects.filter(Prefix=prefix)

    # def get_object_with_key(self, key):
    #     return self.bu

    def delete_all_objects(self):
        """ DANGER: deletes all objects under the bucket"""
        return self.bucket.objects.delete()

if __name__ == '__main__':
    bucket_name = 'immigration-raw-data'
    # s3 = S3Connection()
    # print(s3.create_bucket(bucket_name))
    # print(s3.list_buckets())
    # print(s3.get_bucket('udacity-capstone-mf'))
    # print(s3.delete_bucket('udacity-capstone-mf'))

    s3 = S3Connection()
    bucket = s3.get_bucket(bucket_name)
    print(bucket)
    bucket_conn = S3BucketConnection(bucket)
    print(bucket_conn.get_all_objects())
    s3.upload_file(
        '/Users/marciofernandes/Documents/courses/DE_nanodegree/DE_nanodegree/5. Capstone/immigration_data_sample.csv',
        bucket=bucket_name,
        key='immigration_data_sample.csv')
    print(bucket_conn.get_all_objects())

