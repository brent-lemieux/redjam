import argparse
import configparser
import json
import os
import time
import pandas as pd
import boto3

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

AWS_KEY = os.environ['DW_AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['DW_AWS_SECRET_ACCESS_KEY']
AWS_REGION = 'us-west-2'

CLUSTER_TYPE = config.get("CLUSTER", "CLUSTER_TYPE")
NUM_NODES = int(config.get("CLUSTER", "NUM_NODES"))
NODE_TYPE = config.get("CLUSTER", "NODE_TYPE")

IDENTIFIER = config.get("CLUSTER", "IDENTIFIER")
DBNAME = config.get("CLUSTER", "DBNAME")
USER = config.get("CLUSTER", "USER")
PASSWORD = config.get("CLUSTER", "PASSWORD")
PORT = config.get("CLUSTER", "PORT")

IAM_ROLE_NAME = config.get("CLUSTER", "IAM_ROLE_NAME")


def create_clients():
    """Create EC2, S3, IAM, and Redshift clients.

    Returns:
        ec2_client (boto3.resource) - EC2 client
        s3_client (boto3.resource) - S3 client
        iam_client (boto3.client) - IAM client
        redshift_client (boto3.client) - Redshift client
    """
    ec2_client = boto3.resource(
        'ec2', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    s3_client = boto3.resource(
        's3', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    iam_client = boto3.client(
        'iam', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    redshift_client = boto3.client(
        'redshift', region_name=AWS_REGION, aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
    return ec2_client, s3_client, iam_client, redshift_client


def create_iam_role(iam_client):
    """Create an IAM role for the Redshift cluster to have read only access to
    S3.

    Arguments:
        iam_client (boto3.client) - IAM client

    Returns:
        role_arn (str) - ARN for the IAM Role
    """
    # Create the role if it doesn't already exist.
    try:
        print('Creating IAM Role...')
        redshift_role = iam_client.create_role(
            Path="/",
            RoleName=IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services",
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [
                        {
                            'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}
                        }
                    ],
                    'Version': '2012-10-17'
                }
            )
        )
    except Exception as e:
        print(e)
    # Attach the policy.
    try:
        iam_client.attach_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadonlyAccess"
        )
    except Exception as e:
        print(e)
    # Return the Role ARN.
    role_arn = iam_client.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
    print('Role ARN: %s' % role_arn)
    return role_arn


def create_redshift_cluster(redshift_client, role_arn):
    """Create the Redshift cluster and print properties.

    Arguments:
        redshift_client (boto3.client) - Redshift client
        role_arn (str) - ARN for the IAM Role

    Returns:

    """
    # Create the cluster if it doesn't exist.
    try:
        response = redshift_client.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=NUM_NODES,
            DBName=DBNAME,
            ClusterIdentifier=IDENTIFIER,
            MasterUsername=USER,
            MasterUserPassword=PASSWORD,
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)



def print_cluster_properties(redshift_client):
    """Print the clusters properties.

    Arguments:
        redshift_client (boto3.client) - Redshift client
    """
    cluster_properties = get_cluster_properties(redshift_client)
    print('HOST: %s' % cluster_properties['Endpoint']['Address'])
    property_keys = [
        'ClusterIdentifier', 'NodeType', 'ClusterStatus', 'MasterUsername',
        'DBName', 'Endpoint', 'NumberOfNodes', 'VpcId'
    ]
    property_tuples = [
        (k, v) for k, v in cluster_properties.items() if k in property_keys
    ]
    dfshow = pd.DataFrame(data=property_tuples, columns=['Key', 'Value'])
    print(dfshow)


def get_cluster_properties(redshift_client):
    """Helper function to get the cluster's properties.

    Arguments:
        redshift_client (boto3.client) - Redshift client

    Returns:
        cluster_properties (dict) - cluster properties
    """
    cluster_properties = redshift_client.describe_clusters(
        ClusterIdentifier=IDENTIFIER
    )['Clusters'][0]
    return cluster_properties


def create_infrastructure():
    """Create Redshift infrastructure for this project and set ARN."""
    ec2_client, s3_client, iam_client, redshift_client = create_clients()
    role_arn = create_iam_role(iam_client)
    config.set("IAM_ROLE", "ARN", role_arn)
    create_redshift_cluster(redshift_client, role_arn)
    # Loop until the cluster status becomes "Available"
    status = ""
    while status.lower() != "available":
        cluster_properties = get_cluster_properties(redshift_client)
        status = cluster_properties['ClusterStatus']
        print('Cluster status is %s' % status)
        time.sleep(30)
    print_cluster_properties(redshift_client)


def delete_infrastructure():
    ec2_client, s3_client, iam_client, redshift_client = create_clients()
    redshift_client.delete_cluster(
        ClusterIdentifier=IDENTIFIER,  SkipFinalClusterSnapshot=True
    )


def print_infrastructure():
    _, _, _, redshift_client = create_clients()
    print(cluster_properties(redshift_client))


if __name__ == '__main__':
    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', default=False, type=bool)
    parser.add_argument('--delete', default=False, type=bool)
    parser.add_argument('--print', default=False, type=bool)
    args = parser.parse_args()

    if args.build:
        create_infrastructure()
    if args.delete:
        delete_infrastructure()
    if args.print:
        print_infrastructure()
