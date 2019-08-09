import argparse
import configparser
import json
import os
import time
import pandas as pd
import boto3

config = configparser.ConfigParser()
# Check to see which environment the code is running in.
if 'Brent' in os.uname().nodename:
    config.read('/Users/brent/projects/redjam/dwh.cfg')
else:
    config.read('/usr/local/projects/redjam/dwh.cfg')


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


# BUILD
def create_infrastructure(aws_key, aws_secret):
    """Create Redshift infrastructure for this project and set ARN."""
    ec2_client, s3_client, iam_client, redshift_client = create_clients(
        aws_key, aws_secret
    )
    role_arn = create_iam_role(iam_client)
    create_redshift_cluster(redshift_client, role_arn)
    # Loop until the cluster status becomes "Available"
    status = ""
    while status.lower() != "available":
        cluster_properties = get_cluster_properties(redshift_client)
        status = cluster_properties['ClusterStatus']
        print('Cluster status is %s' % status)
        time.sleep(30)
    set_vpc_properties(ec2_client, cluster_properties['VpcId'])
    print_cluster_properties(redshift_client)


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


def set_vpc_properties(ec2_client, vpc_id):
    """Open incoming TCP port to access the cluster endpoint.

    Arguments:
        ec2_client (boto3.client) - EC2 client
        vpc_id (str) - VPC identifier for Redshift cluster
    """
    try:
        vpc = ec2_client.Vpc(id=vpc_id)
        default_security_group = list(vpc.security_groups.all())[0]
        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(PORT),
            ToPort=int(PORT)
        )
    except Exception as e:
        print(e)


### DELETE
def delete_infrastructure(aws_key, aws_secret):
    """Delete the configured infrastructure if it exists."""
    # Create boto3 clients for AWS resources.
    ec2_client, _, iam_client, redshift_client = create_clients(
        aws_key, aws_secret
    )
    # Get the clusters properties.
    cluster_properties = get_cluster_properties(redshift_client)
    # Clean up resources.
    redshift_client.delete_cluster(
        ClusterIdentifier=IDENTIFIER,  SkipFinalClusterSnapshot=True
    )
    iam_client.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadonlyAccess"
    )
    iam_client.delete_role(RoleName=IAM_ROLE_NAME)


### PRINT
def print_infrastructure(aws_key, aws_secret):
    """Print the configured infrastructure."""
    _, _, _, redshift_client = create_clients(aws_key, aws_secret)
    for k, v in get_cluster_properties(redshift_client):
        print(k, v)


### UTILITIES
def create_clients(aws_key, aws_secret):
    """Create EC2, S3, IAM, and Redshift clients.

    Returns:
        ec2_client (boto3.resource) - EC2 client
        s3_client (boto3.resource) - S3 client
        iam_client (boto3.client) - IAM client
        redshift_client (boto3.client) - Redshift client
    """
    ec2_client = boto3.resource(
        'ec2', region_name=AWS_REGION, aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    s3_client = boto3.resource(
        's3', region_name=AWS_REGION, aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    iam_client = boto3.client(
        'iam', region_name=AWS_REGION, aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    redshift_client = boto3.client(
        'redshift', region_name=AWS_REGION, aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    return ec2_client, s3_client, iam_client, redshift_client


if __name__ == '__main__':
    # Parse command line args.
    parser = argparse.ArgumentParser()
    parser.add_argument('--build', default=False, type=bool)
    parser.add_argument('--delete', default=False, type=bool)
    parser.add_argument('--print', default=False, type=bool)
    args = parser.parse_args()

    if args.build:
        create_infrastructure(AWS_KEY, AWS_SECRET)
    else:
        AWS_KEY = os.environ['DW_AWS_ACCESS_KEY_ID']
        AWS_SECRET = os.environ['DW_AWS_SECRET_ACCESS_KEY']
    if args.delete:
        delete_infrastructure(AWS_KEY, AWS_SECRET)
    if args.print:
        print_infrastructure(AWS_KEY, AWS_SECRET)
