
import os
import logging

import boto3

from aws import config


logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class IAMConnection:
    REGION = config['AWS']['REGION']
    AWS_KEY = config.get('AWS', 'KEY')
    AWS_PASSWORD = config.get('AWS', 'SECRET')
    def __init__(self):
        self.connection = boto3.resource(
            's3',
            region_name = self.REGION,
            aws_access_key_id = os.getenv('AWS_KEY', self.AWS_KEY),
            aws_secret_access_key= os.geten
            )