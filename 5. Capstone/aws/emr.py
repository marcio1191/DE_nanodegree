import os
import logging
from pprint import pprint

import boto3

from aws import config

logger = logging.getLogger(__file__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

class EMRConnection:
    REGION = config['AWS']['REGION']
    AWS_KEY = config.get('AWS', 'KEY')
    AWS_PASSWORD = config.get('AWS', 'SECRET')
    AWS_SESSION_TOKEN = config.get('AWS', 'SESSION_TOKEN')

    # -----------SPARK-------------
    EMR_NAME = config['EMR']['name']
    EMR_LOG_URI = config['EMR']['log_uri']
    S3_SOURCE_BUCKET = config['EMR']['source_bucket_name']
    S3_OUTPUT_BUCKET = config['EMR']['output_bucket_name']

    def __init__(self):
        self.client = boto3.client(
            'emr',
            region_name = self.REGION,
            aws_access_key_id = os.getenv('AWS_KEY', self.AWS_KEY),
            aws_secret_access_key= os.getenv('AWS_PASSWORD', self.AWS_PASSWORD),
            aws_session_token=self.AWS_SESSION_TOKEN if self.AWS_SESSION_TOKEN != '' else None
            )

    def create_spark_cluster_and_run_job(self, keep_alive=False, steps=None, ec2_key_pair='', vpc_subnet_id=''):
        if steps is None:
            steps = []

        # steps = [
        #         {
        #             'Name': 'Setup Debugging',
        #             'ActionOnFailure': 'TERMINATE_CLUSTER',
        #             'HadoopJarStep': {
        #                 'Jar': 'command-runner.jar',
        #                 'Args': ['state-pusher-script']
        #             }
        #         },
        #         # {
        #         #     'Name': 'Setup - Copy Files',
        #         #     'ActionOnFailure': 'CANCEL_AND_WAIT',
        #         #     'HadoopJarStep': {
        #         #         'Jar': 'command-runner.jar',
        #         #         'Args': ['aws',
        #         #                 's3',
        #         #                 'cp',
        #         #                 's3://' + self.S3_SOURCE_BUCKET, '/home/hadoop/',
        #         #                 '--recursive']
        #         #     }
        #         # },
        #         # {
        #         #     'Name': 'Run Spark',
        #         #     'ActionOnFailure': 'CANCEL_AND_WAIT',
        #         #     'HadoopJarStep': {
        #         #         'Jar': 'command-runner.jar',
        #         #         'Args': ['spark-submit',
        #         #                 '/home/hadoop/etl.py',
        #         #                 config['DATALAKE']['input_data'],
        #         #                 config['DATALAKE']['output_data']]
        #         #     }
        #         # }
        #     ]

        cluster_id = self.client.run_job_flow(
            Name=self.EMR_NAME,
            ReleaseLabel='emr-5.36.0',
            LogUri=self.EMR_LOG_URI,
            Applications=[{'Name': 'Spark'},],
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                }
            ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm3.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm3.xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': keep_alive, ## terminate job
                'TerminationProtected': False,
                'Ec2KeyName': ec2_key_pair,
                'Ec2SubnetId': vpc_subnet_id,
            },
            Steps=steps,
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        logger.info('EMR cluster created with the JobFlowId: %s', cluster_id['JobFlowId'])
        return cluster_id


    def describe_cluster(self, id):
        response = self.client.describe_cluster(ClusterId=id)
        logger.debug(pprint(response))
        return response

    def get_cluster_status(self, id):
        response = self.client.describe_cluster(ClusterId=id)
        return response['Cluster']['Status']

    def list_clusters(self):
        return self.client.list_clusters()

    def list_not_terminated_cluster_ids(self):
        clusters = self.client.list_clusters()
        return [d['Id'] for d in clusters['Clusters'] if 'TERMINATED' not in d['Status']['State'] ]


    def list_waiting_clusters(self):
        return self.client.list_clusters(ClusterStates=['WAITING'])['Clusters']

    def terminate_cluster(self, cluster_id):
        response = self.client.terminate_job_flows(JobFlowIds=[cluster_id])
        return response

    def get_master_node_public_dns(self, cluster_id):
        cluster_info = emr.describe_cluster(id=cluster_id)
        return cluster_info['Cluster']['MasterPublicDnsName']

    def get_master_node_address(self, cluster_id):
        return f'hadoop@{self.get_master_node_public_dns(cluster_id)}'

if __name__ == '__main__':
    from aws import ec2
    ec2s = ec2.EC2Session()
    # ec2s.create_ssh_key_pair('emr-cluster', write_file=True)
    emr = EMRConnection()
    # emr.create_spark_cluster_and_run_job(keep_alive=True, ec2_key_pair='emr-cluster')
    emr.list_waiting_clusters()

    # pprint(emr.get_cluster_status('j-3FPGHVRHS3K89'))
    # pprint(emr.get_cluster_status('j-GWSV1CRZT3DT'))

    # emr.terminate_cluster('j-3TB5A8B9IDY6G')
    # pprint(emr.list_waiting_clusters())
    print(emr.list_not_terminated_cluster_ids())
    # emr.list_waiting_clusters()
    # pprint(emr.describe_cluster(id='j-1BJWKPN6ODB5N'))
    # print(emr.get_master_node_public_dns('j-1BJWKPN6ODB5N'))
    # emr.terminate_cluster('j-1BJWKPN6ODB5N')