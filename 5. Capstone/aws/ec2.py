import os
import logging

import boto3

from aws import config


logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class EC2Session:
    REGION = config['AWS']['REGION']
    AWS_KEY = config.get('AWS', 'KEY')
    AWS_PASSWORD = config.get('AWS', 'SECRET')
    AWS_SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN')

    def __init__(self):
        self.resource = boto3.resource(
            'ec2',
            region_name = self.REGION,
            aws_access_key_id = os.getenv('AWS_KEY', self.AWS_KEY),
            aws_secret_access_key= os.getenv('AWS_PASSWORD', self.AWS_PASSWORD),
            aws_session_token=self.AWS_SESSION_TOKEN if self.AWS_SESSION_TOKEN != '' else None,
            )
        self.client = self.resource.meta.client


    def create_ssh_key_pair(self, name, write_file=False, out_dir=os.getcwd()):
        logger.info(f'Creating key pair "{name}"')
        response = self.client.create_key_pair(KeyName=name, KeyType='rsa', KeyFormat='pem')
        logger.debug(response)
        if write_file:
            f_name = f'{name}.pem'
            with open(os.path.join(out_dir, f_name), 'w') as f:
                f.write(response['KeyMaterial'])
        os.chmod(f_name, 400)
        return response['KeyMaterial']

    def delete_ssh_key_pair(self, name):
        logger.info(f'Deleting key pair "{name}"')
        response = self.client.delete_key_pair(KeyName=name)
        logger.debug(response)

    def list_key_pairs(self):
        return self.client.describe_key_pairs()

if __name__ == '__main__':
    ec2 = EC2Session()
    # ec2.create_ssh_key_pair('spark-cluste2r', 'spark-cluster.pem')
    # ec2.delete_ssh_key_pair('spark-cluster')
    # from pprint import pprint
    # pprint(ec2.list_key_pairs())
    print(ec2.list_key_pairs())
    # ec2.delete_ssh_key_pair('spark-cluster')
    # pprint(ec2.list_key_pairs())
